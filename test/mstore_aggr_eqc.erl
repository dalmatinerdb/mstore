-module(mstore_aggr_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0,
                       non_empty_i_or_f_list/0, out/1]).

-compile(export_all).

prop_n_length_chunks() ->
    ?FORALL({L, N}, {list(int()), pos_int()},
            ceiling(length(L) / N) =:= length(n_length_chunks(L, N))).

prop_avg_all() ->
    ?FORALL(L, non_empty_i_or_f_list(),
            [lists:sum(L)/length(L)] == ?B2L(mstore_aggr:avg(?L2B(L), length(L)))).

prop_avg_len() ->
    ?FORALL({L, N}, {non_empty_i_or_f_list(), pos_int()},
            ceiling(length(L)/N) == length(?B2L(mstore_aggr:avg(?L2B(L), N)))).

prop_avg_impl() ->
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            avg(L, N) == mstore_bin:to_list(mstore_aggr:avg(B, N))).

prop_sum() ->
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            sum(L, N) == mstore_bin:to_list(mstore_aggr:sum(B, N))).

%% We need to know about unset values for min!
prop_min() ->
    ?FORALL({{L, _, B}, N}, {i_or_f_array(), pos_int()},
            min_list(L, N) == mstore_bin:to_list(mstore_aggr:min(B, N))).

%% We need to know about unset values for min!
prop_max() ->
    ?FORALL({{L, _, B}, N}, {i_or_f_array(), pos_int()},
            max_list(L, N) == mstore_bin:to_list(mstore_aggr:max(B, N))).

prop_der() ->
    ?FORALL({_, L, B}, i_or_f_array(),
            derivate(L) == mstore_bin:to_list(mstore_aggr:derivate(B))).

avg(L, N) ->
    apply_n(L, N, fun avg_/2).

sum(L, N) ->
    apply_n(L, N, fun sum_/2).

min_list(L, N) ->
    apply_n(L, N, fun min_/2).

max_list(L, N) ->
    apply_n(L, N, fun max_/2).

apply_n(L, N, F) ->
    fix_list([F(SL, N) || SL <- n_length_chunks(L, N)], 0, []).

avg_(L, N) ->
    lists:sum(L) / N.

sum_(L, _N) ->
    lists:sum(L).

min_(L, _N) ->
    case lists:sort([V || {true, V} <- L]) of
        [] ->
            undefined;
        [S | _ ] ->
            S
    end.

max_(L, _N) ->
    case lists:sort([V || {true, V} <- L]) of
        [] ->
            undefined;
        L1 ->
            lists:last(L1)
    end.

fix_list([undefined | T], Last, Acc) ->
    fix_list(T, Last, [Last | Acc]);
fix_list([V | T], _, Acc) ->
    fix_list(T, V, [V | Acc]);
fix_list([],_,Acc) ->
    lists:reverse(Acc).

derivate([]) ->
    [];
derivate([H | T]) ->
    derivate(H, T, []).
derivate(H, [H1 | T], Acc) ->
    derivate(H1, T, [H1 - H | Acc]);
derivate(_, [], Acc) ->
    lists:reverse(Acc).

prop_ceiling() ->
    ?FORALL(F, real(),
            F =< ceiling(F)).

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

-include("eqc_helper.hrl").
