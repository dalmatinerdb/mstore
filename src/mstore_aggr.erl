%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  8 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mstore_aggr).

-export([sum/2, avg/2, derivate/1]).
-include("mstore.hrl").


avg(Data, Count) ->
    avg(Data, 0, 0, Count, Count, <<>>).

avg(R, Last, Sum, 0, Count, Acc) ->
    Avg = Sum/Count,
    Acc1 = <<Acc/binary, ?FLOAT, Avg:?BITS/float>>,
    avg(R, Last, 0, Count, Count, Acc1);
avg(<<?INT, I:?BITS/signed-integer, R/binary>>, _Last, Sum, N, Count, Acc) ->
    avg(R, I, Sum + I, N - 1, Count, Acc);

avg(<<?FLOAT, I:?BITS/float, R/binary>>, _Last, Sum, N, Count, Acc) ->
    avg(R, I, Sum + I, N - 1, Count, Acc);

avg(<<?NONE, 0:?BITS/float, R/binary>>, Last, Sum, N, Count, Acc) ->
    avg(R, Last, Sum + Last, N-1, Count, Acc);

avg(<<>>, _, 0, _Count, _Count, Acc) ->
    Acc;

avg(<<>>, _, Sum, _Missing, Count, Acc) ->
    Avg = Sum/Count,
    <<Acc/binary, ?FLOAT, Avg:?BITS/float>>.

sum(<<>>, _Count) ->
    <<>>;

sum(Data, Count) ->
    case mstore_bin:find_type(Data) of
        integer ->
            sum_int(Data, 0, 0, Count, Count, <<>>);
        float ->
            sum_float(Data, 0.0, 0.0, Count, Count, <<>>);
        unknown ->
            mstore_bin:empty(erlang:max(round(byte_size(Data)/?DATA_SIZE/Count), 1))
    end.

sum_float(R, Last, Sum, 0, Count, Acc) ->
    Acc1 = <<Acc/binary, ?FLOAT, Sum:?BITS/float>>,
    sum_float(R, Last, 0.0, Count, Count, Acc1);
sum_float(<<?FLOAT, I:?BITS/float, R/binary>>, _Last, Sum, N, Count, Acc) ->
    sum_float(R, I, Sum+I, N-1, Count, Acc);
sum_float(<<?NONE, 0:?BITS/float, R/binary>>, Last, Sum, N, Count, Acc) ->
    sum_float(R, Last, Sum + Last, N-1, Count, Acc);
sum_float(<<>>, _, 0.0, _Count, _Count, Acc) ->
    Acc;
sum_float(<<>>, _, Sum, _, _, Acc) ->
    <<Acc/binary, ?FLOAT, Sum:?BITS/float>>.

sum_int(R, Last, Sum, 0, Count, Acc) ->
    Acc1 = <<Acc/binary, ?INT, Sum:?BITS/signed-integer>>,
    sum_int(R, Last, 0, Count, Count, Acc1);
sum_int(<<?INT, I:?BITS/signed-integer, R/binary>>, _, Sum, N, Count, Acc) ->
    sum_int(R, I, Sum+I, N-1, Count, Acc);
sum_int(<<?NONE, 0:?BITS/signed-integer, R/binary>>, Last, Sum, N, Count, Acc) ->
    sum_int(R, Last, Sum+Last, N-1, Count, Acc);
sum_int(<<>>, _, 0, _Count, _Count, Acc) ->
    Acc;
sum_int(<<>>, _, Sum, _, _, Acc) ->
    <<Acc/binary, ?INT, Sum:?BITS/signed-integer>>.

derivate(<<>>) ->
    <<>>;

derivate(<<?INT, I:?BITS/signed-integer, Rest/binary>>) ->
    der_int(Rest, I, <<>>);

derivate(<<?FLOAT, I:?BITS/float, Rest/binary>>) ->
    der_float(Rest, I, <<>>);

derivate(<<?NONE, 0:?BITS/signed-integer, Rest/binary>>) ->
    case mstore_bin:find_type(Rest) of
        integer ->
            der_int(Rest, 0, <<>>);
        float ->
            der_float(Rest, 0.0, <<>>);
        unknown ->
            mstore_bin:empty(round(byte_size(Rest)/?DATA_SIZE))
    end.

der_int(<<?INT, I:?BITS/signed-integer, Rest/binary>>, Last, Acc) ->
    der_int(Rest, I, <<Acc/binary, ?INT, (I - Last):?BITS/signed-integer>>);
der_int(<<?NONE, 0:?BITS/signed-integer, Rest/binary>>, Last, Acc) ->
    der_int(Rest, Last, <<Acc/binary, ?INT, 0:?BITS/signed-integer>>);
der_int(<<>>, _, Acc) ->
    Acc.

der_float(<<?FLOAT, I:?BITS/float, Rest/binary>>, Last, Acc) ->
    der_float(Rest, I, <<Acc/binary, ?FLOAT, (I - Last):?BITS/float>>);
der_float(<<?NONE, 0:?BITS/float, Rest/binary>>, Last, Acc) ->
    der_float(Rest, Last, <<Acc/binary, ?FLOAT, 0:?BITS/float>>);
der_float(<<>>, _, Acc) ->
    Acc.
