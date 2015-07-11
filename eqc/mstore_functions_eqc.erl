-module(mstore_functions_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_helper, [int_array/0, pos_int/0, non_neg_int/0,
                        non_empty_int_list/0, defined_int_array/0]).

-compile(export_all).
%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

size() ->
    choose(1000,2000).

length() ->
    choose(200, 5000).

offset() ->
    choose(0, 5000).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_cout_parts() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            lists:sum([C || {_, C} <-mstore:make_splits(Time, Count, Size, [])]) == Count).

prop_max_size_make_splits() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            length([false || {_, C} <- mstore:make_splits(Time, Count, Size, []), C > Size]) == 0).

prop_incomplete_make_splits() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            length([false || {_, C} <-mstore:make_splits(Time, Count, Size, []),
                   C < Size]) =< 2).
