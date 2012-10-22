%%%-------------------------------------------------------------------
%%% @author Ken Friis Larsen <kflarsen@diku.dk>
%%% @copyright (C) 2011, Ken Friis Larsen
%%% Created : Oct 2011 by Ken Friis Larsen <kflarsen@diku.dk>
%%%-------------------------------------------------------------------
-module(mr).

-export([start/1, stop/1, job/5]).

%%%% Interface

start(N) ->
    {Reducer, Mappers} = init(N),
    {ok, spawn(fun() -> coordinator_loop(Reducer, Mappers) end)}.

stop(Pid) -> stop_async(Pid).

job(CPid, MapFun, RedFun, RedInit, Data) -> 
  job_sync(CPid, MapFun, RedFun, RedInit, Data).

%%%% Internal implementation

init(N) -> Reducer = spawn(fun reducer_loop/0),
  {Reducer, init_mappers(N, Reducer)}.

init_mappers(0, _) -> [];
init_mappers(N, Reducer) -> 
  [spawn(fun() -> mapper_loop(Reducer) end) | init_mappers(N-1,Reducer)].


%% synchronous communication

rpc(Pid, Request) ->
    Pid ! {self(), Request},
    receive
	{Pid, Response} ->
	    Response
    end.

reply(From, Msg) ->
    From ! {self(), Msg}.

reply_ok(From) ->
    reply(From, ok).

reply_ok(From, Msg) ->
    reply(From, {ok, Msg}).

setup(Pid, Fun) ->
  rpc(Pid ,{setup, Fun}).

job_sync(CPid, MapFun, RedFun, RedInit, Data) ->
  rpc(CPid, {job, {MapFun, RedFun, RedInit ,Data}}).

%setup_sync(Pid, Fun) ->
%  rpc(Pid, {setup, Fun}).

%% asynchronous communication

info(Pid, Msg) ->
    Pid ! Msg.

stop_async(Pid) ->
    info(Pid, stop).

data_async(Pid, D) ->
    info(Pid, {data, D}).

setup_async(Pid, Fun) ->
    info(Pid, {setup, Fun}).

%%% Coordinator

coordinator_loop(Reducer, Mappers) ->
  receive
	  {From, stop} ->
	    io:format("~p stopping~n", [self()]),
	    lists:foreach(fun stop_async/1, Mappers),
	    stop_async(Reducer),
	    reply_ok(From);
    {From, {job, JobData}} ->
      {MapFun, RedFun, RedInit, Data} = JobData,
      %%setup the reducer and mappers
      %% Set the reducer into receive mode
      %% Is there a reason to do it synced?
      setup(Reducer, {RedFun, RedInit, length(Data)}),
      lists:foreach(fun(M) -> setup_async(M,MapFun) end,Mappers),
      %% Then perform the mappings
      send_data(Mappers,Data),
      lists:foreach(fun(M) -> stop_async(M) end, Mappers),
      coordinator_loop(Reducer, Mappers, From)
  end.

coordinator_loop(Reducer, Mappers, Sender) ->
    receive
  {From, stop} ->
      io:format("~p stopping~n", [self()]),
	    lists:foreach(fun stop_async/1, Mappers),
	    stop_async(Reducer),
	    reply_ok(From);
  {data, D} ->
      %% Return ok and a result to the creator
      reply_ok(Sender,D),
      %% Return back to idle state
      coordinator_loop(Reducer, Mappers);
  {_, {ok, Msg}} ->
      reply_ok(Sender, Msg),
      coordinator_loop(Reducer, Mappers)
    end.

send_data(Mappers, Data) ->
    send_loop(Mappers, Mappers, Data).

send_loop(Mappers, [Mid|Queue], [D|Data]) ->
    data_async(Mid, D),
    send_loop(Mappers, Queue, Data);
send_loop(_, _, []) -> ok;
send_loop(Mappers, [], Data) ->
    send_loop(Mappers, Mappers, Data).


%%% Reducer

reducer_loop() ->
    receive
	stop -> 
	    io:format("Reducer ~p stopping~n", [self()]),
	    ok;
  {From, {setup, {Fun, Acc, Missing}}} ->
      %%Fun - the anonymous reducer function
      %%Acc - The accumulator
      %%Missing - The number of transmissions missing
      reply_ok(From),
      gather_data_from_mappers(From, Fun, Acc, Missing);
  Unknown ->
      io:format("unknown message in reducer: ~p~n",[Unknown]),
      reducer_loop()
    end.

gather_data_from_mappers(Coordinator, _, Acc, 0) ->
  reply_ok(Coordinator, Acc),
  reducer_loop();
gather_data_from_mappers(Coordinator, Fun, Acc, Missing) ->
    receive
  stop ->
      io:format("Reducer ~p stopping~n", [self()]),
      ok;
  {data, D} -> %Should probably be something else
      %% Reduce the data here
      NewAcc = Fun(D, Acc),
      gather_data_from_mappers(Coordinator, Fun, NewAcc, Missing - 1);
  Unknown ->
      io:format("unknown message in gather_data_from_mappers: ~p~n",[Unknown]),
      gather_data_from_mappers(Coordinator, Fun, Acc, Missing)
    end.


%%% Mapper

mapper_loop(Reducer) ->
    receive
  stop ->
      io:format("Mapper ~p stopping~n", [self()]),
      ok;
  {setup, Fun} ->
      mapper_loop(Reducer, Fun);
  Unknown ->
	    io:format("unknown message in mapper: ~p~n",[Unknown]), 
	    mapper_loop(Reducer)
    end.

mapper_loop(Reducer, Fun) ->
    receive
	stop -> 
	    io:format("Mapper ~p stopping~n", [self()]),
      mapper_loop(Reducer),
	    ok;
  {data, Data} ->
      %%Actually perform data manipulation
      %%and send to reducer
      data_async(Reducer, Fun(Data)),
      mapper_loop(Reducer, Fun);
	Unknown ->
	    io:format("unknown message in mapper: ~p~n",[Unknown]), 
	    mapper_loop(Reducer, Fun)
    end.
