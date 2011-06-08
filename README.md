Cloud Haskell
=============

This is **Cloud Haskell**. It's a Haskell framework for distributed applications. Basically, it's a tool for writing applications that coordinate their work on a cluster of commodity computers or virtual machines. This is useful for providing highly reliable, redundant, long-running services, as well as for building compute-intensive applications that can benefit from lots of hardware.

Cloud Haskell has two interfaces:

* an interface based on message-passing between distributed processes. Think of it as Erlang (or MPI) in Haskell. We call this part the _process layer_.
* a fault-tolerant data-centric interface. We call this part the _task layer_. This layer makes it even easier to build distributed applications; the framework automatically takes care of moving your data around and recovering from hardware failure. This layer can be compared to Google's MapReduce but is in fact more flexible; it's closer to Microsoft's Dryad.

This file contains a (slightly out-of-date) introduction to the process layer. We're working on proper documentation for the task layer, but there are example programs included in the distribution.

We suggest perusing the up-to-date Haddock [documentation](http://www.cl.cam.ac.uk/~jee36/remote/) and the [paper](http://www.cl.cam.ac.uk/~jee36/remote.pdf).

Process layer: an introduction
------------------------------

Many programming languages expose concurrent programming as a shared memory model, wherein multiple, concurrently executing programs, or threads, can examine and manipulate variables common to them all. Coordination between threads is achieved with locks, mutexes, and other synchronization mechanisms. In Haskell, these facilities are available as `MVar`s.

In contrast, languages like Erlang eschew shared data and require that concurrent threads communicate only by message-passing. The key insight of Erlang and languages like it is that reasoning about concurrency is much easier without shared memory. Under a message-passing scheme, a thread provides a  recipient, given as a thread identifier, and a unit of data; that data will be transferred to the recipient's address space and placed in a queue, where it can be retrieved by the recipient. Because data is never shared implicitly, this is a particularly good model for distributed systems.

This framework presents a combined approach to distributed framework: while it provides an Erlang-style message-passing system, it lets the programmer use existing concurrency paradigms from Haskell.

Nodes and processes
-------------------

Location is represented by a _node_. Usually, a node corresponds to an instance of the Haskell runtime system; that is, each independently executed Haskell program exists in its own node. Multiple nodes may run concurrently on a single physical host system, but the intention is that nodes run on separate hosts, to take advantage of more hardware.

The basic unit of concurrency is the _process_ (as distinct from an OS process). A process is a concurrent calculation that can  participate in messaging. There is little overhead involved in starting and executing processes, so programmers can start as many as they need. 

Code that runs in a process is in the `ProcessM` monad.

Process management
------------------

Processes are created with the `spawn` function. Its type signatures will help explain its operation:

```haskell
spawn :: NodeId -> Closure (ProcessM ()) -> ProcessM ProcessId
```

`spawn` takes a `NodeId`, indicating where to run the process, and a `Closure`, indicating which function will start the process. This lets the programmer start arbitrary functions on other nodes, which may be running on other hosts. Actual code is not transmitted to the other node; instead, a function identifier is sent. This works on the assumption that all connected nodes are running identical copies of the compiled Haskell binary (unlike Erlang, which allows new code to be sent to remote nodes at runtime).

We encode the function identifier used to start remote processes in a `Closure`. Closures for remotely-callable functions are automatically generated, and are named after the original function with a `__closure` suffix. Therefore, if I have a function like this:

```haskell
greet :: String -> ProcessM ()
greet name = say ("Hello, " ++ name)
```

I can run it on some node (and get its PID) like this:

```haskell
pid <- spawn someNode (greet__closure "John Baptist")
```

The `greet__closure` symbol here identifies a _closure generator_ and is automatically created by the framework from user-defined functions; see the examples or documentation for more details.

You can send messages to a process given its PID. The `send` function corresponds to Erlang's ! operator.

```haskell
send :: (Serializable a) => ProcessId -> a -> ProcessM ()
```

Given a `ProcessId` and a chunk of serializable data (implementing the `Data.Binary.Binary` type class), we can send a message to the given process. The message will be transmitted across the network if necessary and placed in the process's message queue. Note that `send` will accept any type of data, as long as it implements Binary.

A process can receive messages by calling `expect`:

```haskell
expect :: (Serializable a) => ProcessM a
```

Note that `expect` is also polymorphic; the type of message to receive is usually inferred by the compiler. If a message of that type is in the queue, it will be returned. If multiple messages of that type are in the queue, they will be returned in FIFO order. If there are no messages of that type in the queue, the function will block until such a message arrives.

Channels
--------

A _channel_ provides an alternative to message transmission with `send` and `expect`. While `send` and `expect` allow transmission of messages of any type, channels require messages to be of uniform type. Channels work like a distributed equivalent of Haskell's `Control.Concurrent.Chan`. Unlike regular channels, distributed channels have distinct ends: a single receiving port and at least one sending port. Create a channel with a call to `newChannel`:

```haskell
newChannel :: (Serializable a) => ProcessM (SendPort a, ReceivePort a)
```

The resulting `SendPort` can be used with the `sendChannel` function to insert messages into the channel. The `ReceivePort` can be used to receive messages with 'receiveChannel'. The `SendChannel` can be serialized and sent as part of messages to other processes, which can then write to it; the `ReceiveChannel`, though, cannot be serialized, although it can be accessed from multiple threads on the same node.

Setup and walkthrough
---------------------

Here we'll provide a basic example of how to get started with your first project. 

We'll be running a program that will estimate pi, making use of available computing resources potentially on remote systems. There will be an arbitrary number of nodes, one of which will be designated the master, and the remaining nodes will be slaves. The slaves will estimate pi in such a way that their results can be combined by the master, and an approximation will be output. The more nodes, and the longer they run, the more precise the output.

In more detail: the master will assign each slave a region of the Halton sequence, and the slaves will use elements of the sequence to estimate the ratio of points in a unit square that fall within a unit circle, and that the master will sum these ratios. 

Here's the procedure, step by step.

1. Compile Pi6.hs. If you have the framework installed correctly, it should be sufficient to run:

        ghc --make Pi6

2. Select the machines you want to run the program on, and select one of them to be the master. All hosts must be connected on a local area network. For the purposes of this explanation, we'll assume that you will run your master node on a machine named `masterhost` and you will run two slave nodes each on machines named `slavehost1` and `slavehost2`.

3. Copy the compiled executable Pi6 to some location on each of the three hosts.

4. For each node, we need to create a configuration file. This is plain text file, usually named `config` and usually placed in the same directory with the executable. There are many possible settings that can be given in the configuration file, but only a few are necessary for this example; the rest have sensible defaults. On `masterhost`, create a file named `config` with the following content:

        cfgRole MASTER
        cfgHostName masterhost
        cfgKnownHosts masterhost slavehost1 slavehost2

    On `slavehost1`, create a file named `config` with the following content: 

        cfgRole WORKER
        cfgHostName slavehost1
        cfgKnownHosts masterhost slavehost1 slavehost2

    On `slavehost2`, create a file named `config` with the following content: 

        cfgRole WORKER
        cfgHostName slavehost2
        cfgKnownHosts masterhost slavehost1 slavehost2

A brief discussion of these settings and what they mean:

The `cfgRole` setting determines the node's initial behavior. This is a string which is used to differentiate the two kinds of nodes in this example. More complex distributed systems might have more different kinds of roles. In this case, WORKER nodes do nothing on startup, but just wait from a command from a master, whereas MASTER nodes seek out worker nodes and issue them commands.

The `cfgHostName` setting indicates to each node the name of the host it's running on.

The `cfgKnownHosts` setting provides a list of hosts that form part of this distributed execution. This is necessary so that the master node can find its subservient slave nodes.

Taken together, these three settings tell each node (a) its own name, (b) the names of other nodes and (c) their behavioral relationship.

5. Now, run the Pi6 program twice in each of the worker nodes. There should now be four worker nodes awaiting instructions.

6. To start the execution, run Pi6 on the master node. You should see output like this:

        2011-02-10 11:14:38.373856 UTC 0 pid://masterhost:48079/6/    SAY Starting...
        2011-02-10 11:14:38.374345 UTC 0 pid://masterhost:48079/6/    SAY Telling slave nid://slavehost1:33716/ to look at range 0..1000000
        2011-02-10 11:14:38.376479 UTC 0 pid://masterhost:48079/6/    SAY Telling slave nid://slavehost1:45343/ to look at range 1000000..2000000
        2011-02-10 11:14:38.382236 UTC 0 pid://masterhost:48079/6/    SAY Telling slave nid://slavehost2:51739/ to look at range 2000000..3000000
        2011-02-10 11:14:38.384613 UTC 0 pid://masterhost:48079/6/    SAY Telling slave nid://slavehost2:44756/ to look at range 3000000..4000000
        2011-02-10 11:14:56.720435 UTC 0 pid://masterhost:48079/6/    SAY Done: 31416061416061416061416061

Let's talk about what's going on here.

This output is generated by the framework's logging facility. Each line of output has the following fields, left-to-right: the date and time that the log entry was generated; the importance of the message (in this case 0); the process ID of the generating process; the subsystem or component that generated this message (in this case, SAY indicates that these messages were output by a call to the `say` function); and the body of the message. From these messages, we can see that the master node discovered four nodes running on two remote hosts; for each of them, the master emits a "Telling slave..." message. Note that although we had to specify the host names where the nodes were running in the config file, the master found all nodes running on each of those hosts. The log output also tells us which range of indices of the Halton sequence were assigned to each node. Each slave, having performed its calculation, sends its results back to the master, and when the master has received responses from all slaves, it prints out its estimate of pi and ends. The slave nodes continue running, waiting for another request. At this point, we could run the master again, or we can terminate the slaves manually with Ctrl-C or the kill command.

Contributors
------------

I am grateful for the contributions of the following people to this project:
 
* Alan Mycroft
* Andrew P. Black
* John Hughes 
* John Launchbury 
* Koen Claessen 
* Simon Peyton-Jones
* Thomas van Noort 
* Warren Harris

