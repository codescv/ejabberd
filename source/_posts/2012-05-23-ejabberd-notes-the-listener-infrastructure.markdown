---
layout: post
title: "The Listener Infrastructure (ejabberd hacking series)"
date: 2012-05-23 13:49
comments: true
categories: ejabberd

---

Welcome to my first blog about ejabberd source code hacking. In these series of blogs, I want to take notes about how the [ejabberd](http://www.ejabberd.im/) works, and how to hack it to get customized features.

The source code version in discussion is ejabberd 2.1.10 release, which is the latest stable version at this time.

## Intro
The first step to source hacking is code reading. Ejabberd is a big project (with 80k+lines of erlang code), so it's impossible for us to understand it all at once. Like reading a book, we need to get the key idea first.

What is the key idea of this XMPP(Jabber) server, then? Let's review some of the key usages of a typical XMPP session:

1. Alice started an XMPP client on her computer, which establishes a TCP connection to xmpp.example.org:5222.
2. The server authenticates the client by exchanging XML stanzas.
3. Alice uses the XMPP client to exchange messages/presences/iqs with the server, completing tasks such as instant messaging and presence notifying.

The essential task of the server, then, is to listen on a specific port, wait for clients or other servers to connect to it, and then exchange information using specific data formats(in the XMPP's situation, XML stanzas).

That said, let's examine the listener/socket code of ejabberd first, throwing aside other features along the way.

*Notice*: for the sake of clearity, I intentionally omitted a lot of code which are either unrelated to the topic discussed or only used for error handling. 

## Binding listener ports
In ejabberd, the whole server is packaged into a single OTP application.

The modules related to ejabberd's listener infrastructure are: ejabberd_listener, ejabberd_socket and ejabberd_receiver.

The ejabberd_listener listens on every port specified in the ejabberd configuration file, spawns a process for each port and then accepts the sockets. When it finishes, it starts ejabberd_socket which in turn starts two processes: ejabberd_receiver and logic module according to the config (ejabberd_c2s for 5222, for example). The ejabberd_receiver is responsible for receiving any incoming packets and then forwarding them to the logic module. The logic module parses and handles the packets, and sends responses and requests using the ejabberd_socket utils.

Let's start with the application module:

{% codeblock ejabberd_app.erl lang:erlang %}
start(normal, _Args) ->
    Sup = ejabberd_sup:start_link(),
    ejabberd_listener:start_listeners().
{% endcodeblock %}

In ejabberd_sup:start_link/0, ejabberd_listener:start_link/0 is invoked:

{% codeblock ejabberd_sup.erl lang:erlang %}
init([]) ->
    Listener =
    {ejabberd_listener,
     {ejabberd_listener, start_link, []},
     permanent,
     infinity,
     supervisor,
     [ejabberd_listener]}.
{% endcodeblock %}

Inside ejabberd_listener:init/0, the tcp and udp ports are bound according to the config file:

{% codeblock ejabberd_listener.erl lang:erlang %}
init(_) ->
    ets:new(listen_sockets, [named_table, public]),
    bind_tcp_ports(),
    {ok, { {one_for_one, 10, 1}, []}}.

bind_tcp_ports() ->
    case ejabberd_config:get_local_option(listen) of
        Ls ->
            lists:foreach(
              fun({Port, Module, Opts}) ->
                  bind_tcp_port(Port, Module, Opts)
              end,
              Ls)
    end.

bind_tcp_port(PortIP, Module, RawOpts) ->
    %% portip has the following format: {5222, {0,0,0,0},tcp}
    {Port, IPT, IPS, IPV, Proto, OptsClean} = parse_listener_portip(PortIP, RawOpts),
    _Opts, SockOpts} = prepare_opts(IPT, IPV, OptsClean),
    %% save parsed listener options into ets table
    listen_tcp(PortIP, Module, SockOpts, Port, IPS).

listen_tcp(PortIP, Module, SockOpts, Port, IPS) ->
    gen_tcp:listen(Port, [binary,
                {packet, 0},
                {active, false},
                {reuseaddr, true},
                {nodelay, true},
                {send_timeout, ?TCP_SEND_TIMEOUT},
                {keepalive, true} |
                SockOpts]).
{% endcodeblock %}

After ejabberd_listener:init/0 is finished, all the ports specified in the config file are opened(in the listening state), but not accepting any incoming connections yet.

## Accept incoming connections
Next, the sockets start accepting incoming connections in ejabberd_listener:start_listeners/0:

{% codeblock ejabberd_listener.erl lang:erlang %}
start_listeners() ->
    %% load listeners config from ets table
    Ls2 = lists:map(
        fun({Port, Module, Opts}) ->
            start_listener(Port, Module, Opts)
        end
    end, Listeners).
    
start_listener(Port, Module, Opts) ->
    ChildSpec = {Port,
         {?MODULE, start, [Port, Module, Opts]},
         transient,
         brutal_kill,
         worker,
         [?MODULE]},
    supervisor:start_child(ejabberd_listeners, ChildSpec).
    
start(Port, Module, Opts) ->
    proc_lib:start_link(?MODULE, init, [Port, Module, Opts]).

init(PortIP, Module, RawOpts) ->
    {Port, IPT, IPS, IPV, Proto, OptsClean} = parse_listener_portip(PortIP, RawOpts),
    {Opts, SockOpts} = prepare_opts(IPT, IPV, OptsClean),
    init_tcp(PortIP, Module, Opts, SockOpts, Port, IPS)

init_tcp(PortIP, Module, Opts, SockOpts, Port, IPS) ->
    ListenSocket = listen_tcp(PortIP, Module, SockOpts, Port, IPS),
    proc_lib:init_ack({ok, self()}),
    accept(ListenSocket, Module, Opts).
    
accept(ListenSocket, Module, Opts) ->
    case gen_tcp:accept(ListenSocket) of
    {ok, Socket} ->
        ejabberd_socket:start(Module, gen_tcp, Socket, Opts),
        accept(ListenSocket, Module, Opts);
    {error, Reason} ->
        accept(ListenSocket, Module, Opts)
    end.
{% endcodeblock %}

As we see the code above, the start_listeners/0 call spawns a new process for each listened ports, and then link it to the ejabberd_listeners supervisor, which is started in ejabberd_listener:start_link/0. This may seem odd to you at first, as it DOES for me, since normally a supervisor's callbacks and its workers' callbacks ought to be put in separate modules. Don't write code like that; it causes confusions.

If you are careful enough, you might notice that the sockets are listened twice(once in bind_tcp_ports/0, once in start_listeners/0). I am not sure why it is implemented that way; it works perfectly if I removed one of them.

## Start handling requests
Anyway, let's continue to see what happens when a socket is accepted:

{% codeblock ejabberd_socket.erl lang:erlang %}
start(Module, SockMod, Socket, Opts) ->
    ReceiverMod = ejabberd_receiver,  %% see explanation
    RecPid = ReceiverMod:start(Socket, SockMod, none, MaxStanzaSize),
    SocketData = #socket_state{sockmod = SockMod,
                   socket = Socket,
                   receiver = RecPid},
    case Module:start({?MODULE, SocketData}, Opts) of
        {ok, Pid} ->
            SockMod:controlling_process(Socket, Receiver) of
            ReceiverMod:become_controller(Receiver, Pid);
        {error, _Reason} ->
            ReceiverMod:close(Receiver);
    end;
{% endcodeblock %}

When a new socket is accepted, ejabberd_socket:start/4 is invoked to handle the socket events. ejabberd (brightly) splitted this task into two subtasks: one is to handle all the data transmition(sending and receiving, encapsulating the low-level socket implementation), the other is to parse and handle the message corresponding to the data. Hence, as we see here, a receiver process (ReceiverMod) and a logic handling process (Module) are spawned. The receiver module defaults to ejabberd_receiver, which receives XML stanzas and forwards the stanzas to the logic module. The logic module, on the other hand, may be ejabberd_c2s.erl or ejabberd_s2s depending for different tasks.

What 'SockMod:controlling_process(Socket, Receiver)' does is to direct all data sent to the socket to the receiver mod, which then can handle all the incoming data. After the logic module starts, 'ReceiverMod:become_controller(Receiver, Pid)' is called to let the receiver know where to forward incoming message to.

In a word, ejabberd starts a receiver and a handler when a socket is accepted, the receiver handles all incoming data, does some preprocessing(such as parsing the XML) and forward the message to the handler, the handler decides how to handle the message, and (maybe) use the ejabberd_socket util to send response data. If you want to extend the ejabberd to use other protocols than XMPP: this is the place to start. Write a customized receiver module to parse the protocol, a customized handler module to handle all the requests and you are done. More on this topic later.

## To sum up
We have taken a quick tour through the listeners infrastructure in ejabberd. We learned that the ejabberd uses three modules: ejabberd_listener, ejabberd_socket and ejabberd_receiver to handle all the socket related stuff. ejabberd_listener binds and listens on ports, ejabberd_socket starts the receiver and the handler, and provides utils for outgoing data, the receiver handles and parses all incoming data, and forwards messages to the logic module. The logics, on the other handle, are handled by logic modules accroding to the config file. There are, however, many things that we left out, including:

1. customization of receiver/logic modules.
2. congestion control (shapers).
3. how the logic modules interacts with the socket infrastructure.

So get your hands on the code piece by piece, make discoveries, and have fun! :)



