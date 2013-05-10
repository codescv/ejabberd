---
layout: post
title: "The Architecture of Ejabberd"
date: 2012-06-06 17:39
comments: true
categories: ejabberd
 
---

We have taken a quick tour through the socket infrastructure provided with ejabberd, and wrote a few listeners as an exercise. This time let's take a look at the ejabberd as a whole and try to figure out what it is about.

## Overview
![The ejabberd architecture overview](/images/ejabberd_overview.png "Ejabberd Overview")

Like many servers, ejabberd can inherently be broken up into three layers:

* *The Data Layer.* This layer handles how data get stored in databases, and ensures the integrity and constraints of data. 
* *The Logic Layer.* This layer is the most complicated of all. It handles all the XMPP logics, as well as many other features, e.g.: http bindings, access controls, extensible modules and hooks, etc.
* *The Interface Layer.* This is the layer we have just talked about. It handles all the incoming data from and the outgoing data to the client.

## The Data Layer
Ejabberd primarily use [mnesia](http://www.erlang.org/doc/man/mnesia.html) as its "database". Mnesia is in fact a high-performance key-value pair storage system built into the erlang library. It provides many features such as:

* Replication. Tables may be replicated at several nodes.
* Atomic transactions. A series of table manipulation operations can be grouped into a single atomic transaction.
* Location transparency. Programs can be written without knowledge of the actual location of data.
* Extremely fast real time data searches.

Some of its features, such as replication and realtime searches, make it eligible for an extremely extensible database system. However, ejabberd does not enforce its use; it provides ODBC interfaces to allow utilizing of other databases whenever possible. For example, sometimes it might be more convienient to allow the user to authenticate using data stored in an existing database server, or there may be some relational data that need to be used for some purpose. Generally, mnesia is suitable for real fast key-value searches, but performs pretty badly when you use it for "relational" lookups (using the mnesia:select the wrong way). Avoid relational data whenever possible (use key lookup) if you want to make good use of it.

## The Logic Layer
The logic layer is the main and most import part of the ejabberd system. Some of its functionalities include:

* *Jabber Logics.* The client to server connection (typically on tcp/5222) is handled by the module ejabberd_c2s. The server to server connection (typically on tcp/5269) is handled by the modules ejabberd_s2s, ejabberd_s2s_in, ejabber_s2s_out. The HTTP bindings are handled by the ejabberd_http module.
* *Router.* The router handles the routing of most of the messages, i.e., when a Jabber client sends a <message> to another entity, how is the message routed to the correct destination? First ejabberd determines whether the message is a local or a remote one. It does so by looking at the "to" attribute to see if the host implied by the "to" attribute is hosted in itself. If so, the message is local; and is handled by ejabberd_local; otherwise it is treated as an s2s message.
* *Modules.* In additional to the core Jabber and Router logics, there is a large part of the ejabberd which can be plugged in only when necessary, and they are called _modules_. Modules can be started / stopped dynamically at any time, thus making the ejabberd server highly extensible even at runtime. Modules are widely used for various <iq> extensions (the so-called 'XEP's).
* *Hooks*. You want to change the core logics in the ejabberd server? No problem. Hooks are everywhere to help you. A hook is a way by which you can change the behaviour of ejabberd by injecting your new code into the system, without changing any existing code. For example, if you want to roll up your message filter to filter out messages you don't want, you can add a module hooking to the "filter_packet/3" hook. If you want to keep track of all the messages clients sent, you can write a function hooking to 'user_send_packet/3'.
* *Access Control.* All users (including real jabber client users as well as  administrators) are stored the same way in ejabberd. Their privileges are determined by the _groups_ they belong to. Hence, the access control modules in ejabberd allow us to distinguish users with their groups, thus providing different services for different users.
* *Utils and Libraries.* Some other libraries and utils exist in ejabberd for common purposes, e.g.: XML processing, SASL authentication, Encodings, Logger, etc. It is worth noting the ejabberd_logger is a very good logger module perfectly usable by any other projects.

## The Interface Layer
This layer handles incoming data from and outgoing data from the client. Its main functionaly is to listen on a local port waiting for client connections, establish a connection any clients or servers when necessary, and exchange data. It supports TCP/TLS connections as well as UDP transport, as is described in our last discussions. It serves as the interface between the outer world and the ejabberd server.

















