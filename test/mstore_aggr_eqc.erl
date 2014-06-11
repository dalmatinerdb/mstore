-module(mstore_aggr_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0, non_empty_i_or_f_array/0,
                       non_empty_i_or_f_list/0]).

-compile(export_all).

n_length_chunks_prop() ->
    ?FORALL({L, N}, {list(int()), pos_int()},
            ceiling(length(L) / N) =:= length(n_length_chunks(L, N))).

n_length_chunks_test() ->
    ?assert(eqc:quickcheck(n_length_chunks_prop())).

avg_prop() ->
    ?FORALL(L, non_empty_i_or_f_list(),
            [lists:sum(L)/length(L)] == ?B2L(mstore_aggr:avg(?L2B(L), length(L)))),
    ?FORALL({L, N}, {non_empty_i_or_f_list(), pos_int()},
            ceiling(length(L)/N) == length(?B2L(mstore_aggr:avg(?L2B(L), N)))),
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            begin
                SL = avg(L, N),
                SB = mstore_bin:to_list(mstore_aggr:avg(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

avg_test() ->
    ?assert(eqc:quickcheck(avg_prop())).

sum_prop() ->
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            begin
                SL = sum(L, N),
                SB = mstore_bin:to_list(mstore_aggr:sum(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

sum_test() ->
    ?assert(eqc:quickcheck(sum_prop())).

der_prop() ->
    ?FORALL({_, L, B}, i_or_f_array(),
            begin
                DL = derivate(L),
                DB = mstore_bin:to_list(mstore_aggr:derivate(B)),
                ?WHENFAIL(io:format("~p =/= ~p", [DL, DB]),
                          DL =:= DB)
            end).

der_test() ->
    ?assert(eqc:quickcheck(der_prop())).

avg(L, N) ->
    apply_n(L, N, fun avg_/2).

sum(L, N) ->
    apply_n(L, N, fun sum_/2).

apply_n(L, N, F) ->
    [F(SL, N) || SL <- n_length_chunks(L, N)].

avg_(L, N) ->
    lists:sum(L) / N.

sum_(L, _N) ->
    lists:sum(L).

derivate([]) ->
    [];
derivate([H | T]) ->
    derivate(H, T, []).
derivate(H, [H1 | T], Acc) ->
    derivate(H1, T, [H1 - H | Acc]);
derivate(_, [], Acc) ->
    lists:reverse(Acc).

%% Taken from: http://schemecookbook.org/Erlang/NumberRounding
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.
%% taken from http://stackoverflow.com/questions/12534898/splitting-a-list-in-equal-sized-chunks-in-erlang
n_length_chunks([],_) -> [];
n_length_chunks(List,Len) when Len > length(List) ->
    [List];
n_length_chunks(List,Len) ->
    {Head,Tail} = lists:split(Len,List),
    [Head | n_length_chunks(Tail,Len)].
