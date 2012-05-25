---
layout: post
title: "Writing a Simple Echo Service Module"
date: 2012-05-25 09:08
comments: true
categories: ejabberd

---

In this blog, I will continue my discussion on ejabberd's socket infrastructure.
For the sake of simplicity, let's write a simple echo service module, which receives any packet from the client, and echo the packet back.

## Register our listener
First, we must register our service in the ejabberd's config file:

{% codeblock ejabberd.cfg lang:erlang %}
{listen,
  [
   %% ...
   {5555, echo_service, []},
   %% …
  ]}
{% endcodeblock %}

## Quick and dirty echo service
Our echo service will be listening at 5555/tcp. Now let's write a handler for this port,  we start out with a gen_fsm skeleton:

{% codeblock echo_service.erl lang:erlang %}
%%%-------------------------------------------------------------------
%%% @author Chi Zhang <elecpaoao@gmail.com>
%%% @copyright (C) 2012, Chi Zhang
%%% @doc
%%%  echo service demo
%%% @end
%%% Created : 24 May 2012 by Chi Zhang <elecpaoao@gmail.com>
%%%-------------------------------------------------------------------
-module(echo_service).

-behaviour(gen_fsm).

%% API
-export([start_link/2]).

-export([start/2,
         socket_type/0]).
     
%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
     handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-include("ejabberd.hrl").

-record(state, {sockmod, csock, opts}).

%%%===================================================================
%%% API
%%%===================================================================

start(SockData, Opts) ->
    start_link(SockData, Opts).

socket_type() ->
    raw.

start_link(SockData, Opts) ->
    gen_fsm:start_link(?MODULE, [SockData, Opts], []).

%%%===================================================================
%%% gen_fsm
%%%===================================================================

init([{SockMod, CSock}, Opts]) ->
    ?ERROR_MSG("start with sockmod: ~p csock: ~p opts: ~p", [SockMod, CSock, Opts]),
    State = #state{sockmod=SockMod, csock=CSock, opts=Opts},
    activate_socket(State),
    {ok, state_name, State}.

state_name(_Event, State) ->
    {next_state, state_name, State}.

state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info({_, CSock, Packet}, StateName, #state{sockmod=SockMod}=State) ->
    ?ERROR_MSG("received: ~p", [Packet]),
    SockMod:send(CSock, Packet),
    activate_socket(State),
    {next_state, StateName, State};
handle_info({tcp_closed, _CSock}, _StateName, State) ->
    ?ERROR_MSG("client closed: ~p", [State]),
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    ?ERROR_MSG("received: ~p", [_Info]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ?ERROR_MSG("terminated ~p", [_Reason]),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

activate_socket(#state{csock=CSock}) ->
    inet:setopts(CSock, [{active, once}]).

{% endcodeblock %}

Pretty simple, right? With only very few lines of the default gen_fsm code changed,  we have a fully working echo service!

The first thing you may have noticed is the start/2 and socket_type/0 call. Why is this neccessary? Recall from ejabberd_socket:

{% codeblock ejabberd_socket.erl lang:erlang %}
start(Module, SockMod, Socket, Opts) ->
    case Module:socket_type() of
    xml_stream ->
        MaxStanzaSize =
        case lists:keysearch(max_stanza_size, 1, Opts) of
            {value, {_, Size}} -> Size;
            _ -> infinity
        end,
        {ReceiverMod, Receiver, RecRef} =
        case catch SockMod:custom_receiver(Socket) of
            {receiver, RecMod, RecPid} ->
            {RecMod, RecPid, RecMod};
            _ ->
            RecPid = ejabberd_receiver:start(
                   Socket, SockMod, none, MaxStanzaSize),
            {ejabberd_receiver, RecPid, RecPid}
        end,
        SocketData = #socket_state{sockmod = SockMod,
                       socket = Socket,
                       receiver = RecRef},
        case Module:start({?MODULE, SocketData}, Opts) of
        {ok, Pid} ->
            case SockMod:controlling_process(Socket, Receiver) of
            ok ->
                ok;
            {error, _Reason} ->
                SockMod:close(Socket)
            end,
            ReceiverMod:become_controller(Receiver, Pid);
        {error, _Reason} ->
            SockMod:close(Socket),
            case ReceiverMod of
            ejabberd_receiver ->
                ReceiverMod:close(Receiver);
            _ ->
                ok
            end
        end;
    independent ->
        ok;
    raw ->
        case Module:start({SockMod, Socket}, Opts) of
        {ok, Pid} ->
            case SockMod:controlling_process(Socket, Pid) of
            ok ->
                ok;
            {error, _Reason} ->
                SockMod:close(Socket)
            end;
        {error, _Reason} ->
            SockMod:close(Socket)
        end
    end.
{% endcodeblock %}

As is shown above, if the Module:socket_type() returns the atom 'raw', then the module will be used without ejabberd_receiver, which is kinda what we want, because we want to take control over everything. After ejabberd_socket calls Module:start/2, passing the socket module(gen_tcp here) and the socket, it will call SockMod:controlling_process to direct any messages with the socket to the Pid returned by Module:start/2, which is, in our case, the echo_service gen_fsm process.

When the echo_service fsm starts, the socket is in passive mode, that is, it won't get any data until the recv() function is called. As a result, we set the socket to be active once, and the data can be received. Everytime we receive a new packet, we generate and send response, and then set the socket to be active once again.

Finally, don't forget to turn off the FSM when {tcp_closed, Sock} is received. That prevents you from leaking processes.

That should sound reasonable enough. But wait, what if we want our echo service to be working on UDP also?

## Adding UDP Transport
Simple. Let's see how UDP sockets in ejabberd are handled:

{% codeblock ejabberd_listener.erl lang:erlang %}
init_udp(PortIP, Module, Opts, SockOpts, Port, IPS) ->
    case gen_udp:open(Port, [binary,
			     {active, false},
			     {reuseaddr, true} |
			     SockOpts]) of
	{ok, Socket} ->
	    %% Inform my parent that this port was opened succesfully
	    proc_lib:init_ack({ok, self()}),
	    udp_recv(Socket, Module, Opts);
	{error, Reason} ->
	    socket_error(Reason, PortIP, Module, SockOpts, Port, IPS)
    end.
    
udp_recv(Socket, Module, Opts) ->
    case gen_udp:recv(Socket, 0) of
	{ok, {Addr, Port, Packet}} ->
	    case catch Module:udp_recv(Socket, Addr, Port, Packet, Opts) of
		{'EXIT', Reason} ->
		    ?ERROR_MSG("failed to process UDP packet:~n"
			       "** Source: {~p, ~p}~n"
			       "** Reason: ~p~n** Packet: ~p",
			       [Addr, Port, Reason, Packet]);
		_ ->
		    ok
	    end,
	    udp_recv(Socket, Module, Opts);
	{error, Reason} ->
	    ?ERROR_MSG("unexpected UDP error: ~s", [format_error(Reason)]),
	    throw({error, Reason})
    end.
{% endcodeblock %}

That's pretty neat. Instead of spawning a new process for every new connection like the tcp, udp sockets are handled by only one process for each port. 

To use UDP transport, we simply add udp_recv to our Module:

{% codeblock echo_service.erl lang:erlang %}
udp_recv(Socket, Addr, Port, Packet, Opts) ->
    ?ERROR_MSG("udp receive: socket ~p addr ~p port ~p packet ~p opts ~p", [Socket, Addr, Port, Packet, Opts]),
    gen_udp:send(Socket, Addr, Port, Packet).
{% endcodeblock %}

That's enough for most purposes, but you must be careful: if the Module:udp_recv/5 call blocks, it blocks any other data to be handled. Hence, in real life applications, get ready to spawn multiple processes to handle the UDP requests!

## Using customized socket options
The ejabberd_listener's listen options fits our needs in most cases. What if we want customized socket options, other than what the default options, say, we want {packet, 4} to be set before we receive any data from the socket?

That's easy. First add the options in the config file:

{% codeblock ejabberd.cfg lang:erlang %}
{listen,
  [
   %% ...
   {5556, echo_service, [{packet, 4}]},
   %% …
  ]}
{% endcodeblock %}

Then we add a setopts step in our echo service module:

{% codeblock echo_service.erl lang:erlang %}
init([{SockMod, CSock}, Opts]) ->
    ?ERROR_MSG("start with sockmod: ~p csock: ~p opts: ~p", [SockMod, CSock, Opts]),
    State = #state{sockmod=SockMod, csock=CSock, opts=Opts},
    set_opts(State),
    activate_socket(State),
    {ok, state_name, State}.
    
set_opts(#state{csock=CSock, opts=Opts}) ->
    Opts1 = lists:filter(fun(inet) -> false;
			    ({packet, _}) -> true;
			    ( _ ) -> false
			 end, Opts),
    inet:setopts(CSock, Opts1).
{% endcodeblock %}

We have added a filter to filter the options provided, allowing only valid options to be set. Now the echo service listening on 5556/tcp will require a 4-octet header stating the whole packet length, cool isn't it?

## To sum up
We have written a very simple echo service to learn how to use the ejabberd's socket infrastructure. To write a simple TCP service, we only need to implement the socket_type() to return raw, and spawn a process handling the socket in Mod:start/2. To write a simple UDP service, we only need to provide a udp_recv/5 callback. Things we haven't covered yet:

1. What about the TLS transport ? (hint:use the tls module included in ejabberd.)
2. How to separte socket receiving data and socket handling logic? (hint: start and return your own receiver in your Mod:start).
3. How to use the builtin ejabberd_receiver and ejabberd_socket? (hint: return xml_stream for socket_type/0).

The above questions are left out as an exercise. Hack on!

*Note*: All the code in this blog can be accessed at: [git://github.com/eshock/ejabberd.git](git://github.com/eshock/ejabberd.git) on branch echo_service.









