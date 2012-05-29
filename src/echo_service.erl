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

-export([udp_recv/5]).

%% gen_fsm callbacks
-export([init/1, process/2, process/3, handle_event/3,
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
    xml_stream.

udp_recv(Socket, Addr, Port, Packet, Opts) ->
    ?ERROR_MSG("udp receive: socket ~p addr ~p port ~p packet ~p opts ~p", [Socket, Addr, Port, Packet, Opts]),
    gen_udp:send(Socket, Addr, Port, Packet).

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(SockData, Opts) ->
    gen_fsm:start_link(?MODULE, [SockData, Opts], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([{SockMod, CSock}, Opts]) ->
    ?ERROR_MSG("start with sockmod: ~p csock: ~p opts: ~p", [SockMod, CSock, Opts]),
    State = #state{sockmod=SockMod, csock=CSock, opts=Opts},
    NewState = set_opts(State),
    {ok, process, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
process({xmlstreamelement,El}, #state{sockmod=SockMod, csock=CSock} = State) -> 
    ?ERROR_MSG("element: ~p ~p ~p", [SockMod, CSock, El]), 
    SockMod:send(CSock, xml:element_to_binary(El)),
    {next_state, process, State};
process(Event, State) ->
    ?ERROR_MSG("event ~p ~p", [Event, State]),
    {next_state, process, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
process(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, process, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({_, _, Packet}, StateName, #state{sockmod=SockMod, csock=CSock}=State) ->
    case SockMod of
        tls ->
            case tls:recv_data(CSock, Packet) of
                {_, <<>>} ->
                    ok;
                {_, Data} ->
                    SockMod:send(CSock, Data)
            end;
        _ ->
            SockMod:send(CSock, Packet)
    end,
    activate_socket(State),
    {next_state, StateName, State};
handle_info({tcp_closed, _CSock}, _StateName, State) ->
    ?ERROR_MSG("client closed: ~p", [State]),
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    ?ERROR_MSG("received: ~p", [_Info]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ?ERROR_MSG("terminated ~p", [_Reason]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
            TLSOpts = lists:filter(fun({certfile, _}) -> true;
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
                                    ({ip, _}) -> false;
                                    (_) -> true
                                 end, Opts),
            inet:setopts(CSock, Opts1),
            activate_socket(State),
            State
    end.





