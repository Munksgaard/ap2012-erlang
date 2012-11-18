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


stop(Pid) -> rpc(Pid, stop).

job(CPid, MapFun, RedFun, RedInit, Data) ->
    rpc(CPid, {start, MapFun, RedFun, RedInit, Data}).

%%%% Internal implementation

init(N) ->
    Reducer = spawn(fun reducer_loop/0),
    Mappers = init_mappers(N, Reducer),
    {Reducer, Mappers}.

init_mappers(0, _) -> [];
init_mappers(N, Reducer) -> [spawn(fun() -> mapper_loop(Reducer, 
                                                        fun() -> error("Mapper not initialized") end) 
                                   end) 
                             | init_mappers(N-1, Reducer)].

%% Synchronous communication

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
            lists:foreach(fun stop_async/1, Mappers),
            stop_async(Reducer),
            reply_ok(From);
        {From, {start, MapFun, RedFun, RedInit, Data}} ->
            setup_async(Reducer, {self(), RedFun, RedInit, length(Data)}),
            setup_mappers(Mappers, MapFun),
            send_data(Mappers, Data),
            reply(From, coordinator_receive_result()),
            coordinator_loop(Reducer, Mappers);
        Unknown -> 
            io:format("coordinator_loop unknown message: ~p~n",[Unknown]),
            coordinator_loop(Reducer, Mappers)
    end.

coordinator_receive_result() ->
    receive
        {_, {ok, Result}} ->
            {ok, Result};
        Unknown ->
            io:format("coordinator_receive_result unknown message: ~p~n",[Unknown]),
            coordinator_receive_result()
    end.

setup_mappers([], _) -> ok;
setup_mappers([Mapper | Mappers], Fun) ->
    setup_async(Mapper, Fun),
    setup_mappers(Mappers, Fun).

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
            ok;
        {setup, {CPid, RedFun, RedInit, Count}} ->
            reply_ok(CPid, gather_data_from_mappers(RedFun, RedInit, Count)),
            reducer_loop();
        Unknown ->
            io:format("reducer_loop unknown message: ~p~n", [Unknown]),
            reducer_loop()
    end.

gather_data_from_mappers(_, Acc, 0) -> Acc;
gather_data_from_mappers(Fun, Acc, Missing) ->
    receive
        stop ->
            io:format("reducer ~p stopping~n", [self()]),
            ok;
        {data, D} ->
            gather_data_from_mappers(Fun, 
                                     Fun(D, Acc),
                                     Missing - 1);
        Unknown ->
            io:format("gather_data_from_mappers unknown message: ~p~n",[Unknown]),
            gather_data_from_mappers(Fun, Acc, Missing)
    end.

%%% Mapper

mapper_loop(Reducer, Fun) ->
    receive
        stop ->
            io:format("Mapper ~p stopping~n", [self()]),
            ok;
        {setup, FunNew} ->
            mapper_loop(Reducer, FunNew);
        {data, D} ->
            data_async(Reducer, Fun(D)),
            mapper_loop(Reducer, Fun);
        Unknown ->
            io:format("mapper_loop unknown message: ~p~n",[Unknown]),
            mapper_loop(Reducer, Fun)
    end.
