---
layout: post
title: "More on Echo Service: Using TLS and XML Streams"
date: 2012-05-29 15:27
comments: true
categories: 

---

## Using TLS
OK. We have written a simple echo service, serving both on tcp and udp. Now we want our echo service to be working for TLS, too.

The tls module fits well into implementing TLS based secure connections, for it has the following advantages over the defaut erlang ssl module:

1. It is implemented using C and has better performance.
2. It supports starttls i.e. start tls over the tcp connection depending on situations, without having to re-establish a new connection.

To enable tls connections, use tls:tcp_to_tls/2 to transform a tcp socket to a tls one:

{% codeblock echo_service.erl lang:erlang %}
init([{SockMod, CSock}, Opts]) ->
    ?ERROR_MSG("start with sockmod: ~p csock: ~p opts: ~p", [SockMod, CSock, Opts]),
    State = #state{sockmod=SockMod, csock=CSock, opts=Opts},
    NewState = set_opts(State),
    {ok, state_name, NewState}.

handle_info({ _, _, Packet}, StateName, #state{sockmod=SockMod, csock=CSock}=State) ->
    case SockMod of
        tls ->
            case tls:recv_data(CSock, Packet) of
                { _, <<>>} ->
                    ok;
                { _, Data} ->
                    SockMod:send(CSock, Data)
            end;
        _ ->
            SockMod:send(CSock, Packet)
    end,
    activate_socket(State),
    {next_state, StateName, State};
    
activate_socket(#state{csock={tlssock, _, _}=TLSSock}) ->
    tls:setopts(TLSSock, [{active, once}]);
activate_socket(#state{csock=CSock}) ->
    inet:setopts(CSock, [{active, once}]).

set_opts(#state{csock=CSock, opts=Opts} = State) ->
    TLSEnabled = lists:member(tls, Opts),
    if
        TLSEnabled ->
            TLSOpts = lists:filter(fun({certfile, _ }) -> true;
                                      ( _ ) -> false
                                   end, 
                                   [verify_none | Opts]),
            {ok, TLSSock} = tls:tcp_to_tls(CSock, TLSOpts),
            NewState = State#state{sockmod=tls, csock=TLSSock},
            activate_socket(NewState),
            NewState;
        true ->
            Opts1 = lists:filter(fun(inet) -> false;
                                    (tls) -> false;
                                    ({ ip, _ }) -> false;
                                    ( _ ) -> true
                                 end, Opts),
            inet:setopts(CSock, Opts1),
            activate_socket(State),
            State
    end.
{% endcodeblock %}

in set_opts/1, we use tls:tcp_to_tls/2 to transform the accepted tcp socket in to a tls socket, then we use tls:recv_data/2 to receive all the tls data. tls:tls_recv_data will automatically do the handshakes needed, returning data if presents (handshake data excluded). Finally, we use tls:send/2 to send any data back to the client.

*Note*: complete code listing available from [git://git@github.com:eshock/ejabberd.git](git@github.com:eshock/ejabberd.git) on branch _echo_service_.

## Using xml_stream
Now let's take our echo service up to the next level: what about receiving xml streams as input , and echoing xml stanzas?

That's also very simple.

{% codeblock echo_service.erl lang:erlang %}
socket_type() ->
    xml_stream.

init([{SockMod, CSock}, Opts]) ->
    ?ERROR_MSG("start with sockmod: ~p csock: ~p opts: ~p", [SockMod, CSock, Opts]),
    State = #state{sockmod=SockMod, csock=CSock, opts=Opts},
    NewState = set_opts(State),
    {ok, process, NewState}.
    
process({xmlstreamelement,El}, #state{sockmod=SockMod, csock=CSock} = State) -> 
    ?ERROR_MSG("element: ~p ~p ~p", [SockMod, CSock, El]), 
    SockMod:send(CSock, xml:element_to_binary(El)),
    {next_state, process, State};
process(Event, State) ->
    ?ERROR_MSG("event ~p ~p", [Event, State]),
    {next_state, process, State}.
    
activate_socket(#state{sockmod=ejabberd_socket}) ->
    ok;
activate_socket(#state{sockmod=tls, csock=TLSSock}) ->
    tls:setopts(TLSSock, [{active, once}]);
activate_socket(#state{sockmod=gen_tcp, csock=CSock}) ->
    inet:setopts(CSock, [{active, once}]).

set_opts(#state{sockmod=ejabberd_socket}=State) ->
    State;
set_opts(#state{csock=CSock, opts=Opts} = State) ->
    TLSEnabled = lists:member(tls, Opts),
    if
        TLSEnabled ->
            TLSOpts = lists:filter(fun({certfile, _ }) -> true;
                                      (_) -> false
                                   end, 
                                   [verify_none | Opts]),
            {ok, TLSSock} = tls:tcp_to_tls(CSock, TLSOpts),
            NewState = State#state{sockmod=tls, csock=TLSSock},
            activate_socket(NewState),
            NewState;
        true ->
            Opts1 = lists:filter(fun(inet) -> false;
                                    (tls) -> false;
                                    ({ip, _ }) -> false;
                                    ( _ ) -> true
                                 end, Opts),
            inet:setopts(CSock, Opts1),
            activate_socket(State),
            State
    end.

{% endcodeblock %}

We first change the socket_type() to return xml_stream, which tells ejabberd to use ejabberd_receiver as our receiver. Then we override the fsm state call back process/2 to process any incoming xml stanzas. Note that we do no-op for activate_socket and set_opts, for any incoming data are automatically taken care of by the ejabberd_receiver module. 

To test it, let's run nc to connect to the 5555 port:

    nc localhost 5555
    <?xml version='1.0'?><stream:stream to="localhost" xmlns="jabber:client" xmlns:stream="http://etherx.jabber.org/streams" version="1.0">
    <body>hello</body>
    
If everything goes well, the server answers with reply:
    
    <body>hello</body>

*Note*: complete code listing available at: [git://git@github.com:eshock/ejabberd.git](git@github.com:eshock/ejabberd.git) on branch _xml_stream_echo_service_.

## To sum up
We have modified our echo service module to accept tls connections as well as xml_stream stanzas. Next time we'll be talking about something else, but also fun! 





