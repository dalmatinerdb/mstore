%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2016, Heinz N. Gies
%%% @doc MStore file module.
%%%
%%% @end
%%% Created :  5 Dec 2016 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mfile).

-export([
         open/1,
         open/2,
         open/4,
         metrics/1,
         size/1,
         read/5,
         write/5,
         close/1,
         fold/5,
         count/1,
         fold_idx/3
        ]).

-export_type([mfile/0, fold_fun/0]).

-include_lib("mmath/include/mmath.hrl").

-define(OPTS, [raw, binary]).

-record(mfile, {
          name,
          file,
          offset,
          size,
          index = btrie:new(),
          next = 0
         }).
-opaque mfile() :: #mfile{}.

-type fold_fun() :: fun((Metric :: binary(),
                         Offset :: non_neg_integer(),
                         Data :: binary(), AccIn :: any()) -> AccOut :: any()).

-type open_error_reason() :: file:posix() | badarg | system_limit | 
                             offset_missmatch | size_missmatch.

-type read_error_reason() :: file:posix() | badarg | terminated | not_found.

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Opens a metric file.
%% @end
%%--------------------------------------------------------------------

-spec open(file:filename_all()) ->
                  {ok, mfile()} |
                  {error, open_error_reason()}.
open(File) ->
    open(File, read).

-spec open(file:filename_all(), read | write) ->
                  {ok, mfile()} |
                  {error, open_error_reason()}.
open(File, Mode) ->
    FileOpts = case Mode of
                   read ->
                       [read | ?OPTS];
                   write ->
                       [read, write | ?OPTS]
               end,
    ReadIdx = read_idx(File),
    case ReadIdx of
        {Offset, Size, Idx, Next} ->
            case file:open(File ++ ".mstore", FileOpts) of
                {ok, F} ->
                    {ok, #mfile{index=Idx, name=File, file=F, offset=Offset,
                                size=Size, next=Next}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec open(file:filename_all(), non_neg_integer(), pos_integer(), read | write) ->
                  {ok, mfile()} |
                  {error, open_error_reason()}.
open(File, Offset, Size, Mode) when Offset >= 0, Size > 0 ->
    FileOpts = case Mode of
                   read ->
                       [read | ?OPTS];
                   write ->
                       [read, write | ?OPTS]
               end,
    ReadIdx = read_idx(File),
    case ReadIdx of
        {O, S, Idx, Next} when Offset =:= O,
                         Size =:= S ->
            case file:open(File ++ ".mstore", FileOpts) of
                {ok, F} ->
                    {ok, #mfile{index=Idx, name=File, file=F, offset=Offset,
                                size=Size, next=Next}};
                E ->
                    E
            end;
        {O, _, _, _} when Offset =/= O ->
            {error, offset_missmatch};
        {_, S, _, _} when Size =/= S ->
            {error, size_missmatch};
        _E ->
            case file:open(File ++ ".mstore", [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mfile{name=File, file=F, offset=Offset,
                               size=Size, next=0},
                    file:write_file(File ++ ".idx",
                                    <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE>>),
                    {ok, M};
                E ->
                    E
            end
    end.

-spec metrics(mfile()) ->
                    btrie:btrie().
metrics(#mfile{index = Index}) ->
    Index.

-spec size(mfile()) ->
                  pos_integer().
size(#mfile{size = Size}) ->
    Size.
%%--------------------------------------------------------------------
%% @doc
%% 
%% @end
%%--------------------------------------------------------------------
-spec read(mfile(), pos_integer(), binary(), non_neg_integer(), pos_integer()) ->
                  {ok, binary()} |
                  {error, read_error_reason()} |
                  eof.

read(#mfile{offset=Offset, size=S, file=F, index=Idx},
     DataSize, Metric, Position, Count)
  when Position >= Offset,
       (Position - Offset) + Count =< S ->
    case btrie:find(Metric, Idx) of
        error ->
            {error, not_found};
        {ok, Pos} ->
            Base = Pos * S * DataSize,
            P = Base+((Position - Offset) * DataSize),
            file:pread(F, P, Count * DataSize)
    end.

%%--------------------------------------------------------------------
%% @doc
%% 
%% @end
%%--------------------------------------------------------------------
-spec write(mfile(), pos_integer(), binary(), non_neg_integer(), binary()) ->
                   {ok | {error, atom()}, mfile()}.

write(M=#mfile{offset=Offset, size=S},
      DataSize, Metric, Position, Value)
  when is_binary(Value),
       Position >= 0,
       DataSize > 0,
       Position >= Offset,
       (Position - Offset) + (byte_size(Value) div DataSize) =< S ->
    do_write(M, DataSize, Metric, Position, Value).

%%--------------------------------------------------------------------
%% @doc
%% Closes a metric file.
%% @end
%%--------------------------------------------------------------------
-spec close(mfile()) -> ok.

close(#mfile{file=F}) ->
    file:close(F).

%%--------------------------------------------------------------------
%% @doc
%% 
%% @end
%%--------------------------------------------------------------------
-spec fold(file:filename_all() | mfile(), pos_integer(), fold_fun(), pos_integer(), term()) -> term().

fold(#mfile() = MFile, DataSize, Fun, Chunk, Acc) ->
    lists:foldl(fun(M, AccIn) ->
                        serialize_metric(MFile, DataSize, M, Fun, Chunk, AccIn)
                end, Acc, store_metrics(MFile));
fold(BaseName, DataSize, Fun, Chunk, Acc) ->
    {ok, MFile} = open_store(BaseName),
    Res = fold(MFile, DataSize, Fun, Chunk, Acc),
    close(MFile),
    Res.

%%--------------------------------------------------------------------
%% @doc
%% Count number of elementns in give index filename
%% @end
%%--------------------------------------------------------------------
-spec count(mfile() | file:filename_all()) -> non_neg_integer().

count(#mfile{index = BT}) ->
    btrie:size(BT);
count(RootName) ->
    R = fold_idx(fun({init, _Offset, _Size}, Acc) ->
                         Acc;
                    ({entry, _M}, Acc) ->
                         Acc + 1
                 end, 0, RootName),
    case R of
        {error, Reason} ->
            io:format("Could not read index file ~s.idx: ~p~n", [RootName, Reason]),
            0;
        N when is_number(N) ->
            N
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fold over each index element in given index file.
%% @end
%%--------------------------------------------------------------------
-spec fold_idx(function(), term(), string()) -> term().

fold_idx(Fun, Acc0, RootName) ->
    fold_idx(Fun, Acc0, 4*1024, RootName).


%%--------------------------------------------------------------------
%% Private functions.
%%--------------------------------------------------------------------




store_metrics(#mfile{index=M}) ->
    btrie:fetch_keys(M).

serialize_metric(MFile, DataSize, Metric, Fun, infinity, Acc) ->
    #mfile{offset=O,size=S} = MFile,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,

    case read(MFile, DataSize, Metric, O, S) of
        {ok, Data} ->
            serialize_binary(DataSize, Data, Fun1, Acc, O, <<>>);
        eof ->
            Acc
    end;

serialize_metric(MFile, DataSize, Metric, Fun, Chunk, Acc) ->
    serialize_metric(MFile, DataSize, Metric, Fun, 0, Chunk, Acc).

%% If we've read everything (Start = Size) we just return the acc
serialize_metric(#mfile{size = _Size},
                 _DataSize, _Metric, _Fun, _Start, _Chunk, Acc) when _Start == _Size ->
    Acc;

%% If we have at least 'Chunk' left to read
serialize_metric(MFile = #mfile{offset = O,
                                size = Size},
                 DataSize, Metric, Fun, Start, Chunk, Acc)
  when Start + Chunk < Size ->
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset + Start, Data, AccIn)
           end,
    case read(MFile, DataSize, Metric, O1, Chunk) of
        {ok, Data} ->
            Acc1 = serialize_binary(DataSize, Data, Fun1, Acc, O1, <<>>),
            serialize_metric(MFile, DataSize, Metric, Fun, Start + Chunk, Chunk, Acc1);
        eof ->
            Acc
    end;

%% We don't have a full chunk left to read.
serialize_metric(MFile = #mfile{offset = O,
                                size = Size},
                 DataSize, Metric, Fun, Start, _Chunk, Acc) ->
    Chunk = Size - Start,
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset + Start, Data, AccIn)
           end,
    case read(MFile, DataSize, Metric, O1, Chunk) of
        {ok, Data} ->
            serialize_binary(DataSize, Data, Fun1, Acc, O1, <<>>);
        eof ->
            Acc
    end.

serialize_binary(_DataSize, <<>>, _Fun, FunAcc, _O, <<>>) ->
    FunAcc;
serialize_binary(_DataSize, <<>>, Fun, FunAcc, O, Acc) ->
    Fun(O, Acc, FunAcc);
serialize_binary(DataSize, <<?NONE:?TYPE_SIZE, R/binary>>, Fun, FunAcc, O, <<>>) ->
    VSize =DataSize - 1,
    <<_:VSize/binary, R1/binary>> = R,
    serialize_binary(DataSize, R1, Fun, FunAcc, O+1, <<>>);
serialize_binary(DataSize, <<?NONE:?TYPE_SIZE, R/binary>>, Fun, FunAcc, O, Acc) ->
    FunAcc1 = Fun(O, Acc, FunAcc),
    VSize =DataSize - 1,
    <<_:VSize/binary, R1/binary>> = R,
  serialize_binary(DataSize, R1, Fun, FunAcc1, O+mmath_bin:length(Acc)+1, <<>>);
serialize_binary(DataSize, R, Fun, FunAcc, O, Acc) ->
    <<V:DataSize/binary, R1/binary>> = R,
    serialize_binary(DataSize, R1, Fun, FunAcc, O, <<Acc/binary, V/binary>>).

open_store(BaseName) ->
    case read_idx(BaseName) of
        {Offset, Size, Idx, Next} ->
            case file:open(BaseName ++ ".mstore", [read | ?OPTS]) of
                {ok, F} ->
                    {ok, #mfile{index=Idx, name=BaseName, file=F, offset=Offset,
                                size=Size, next=Next}};
                E ->
                    E
            end;
        E ->
            io:format("Error opening file ~s.idx: ~p~n", [BaseName, E]),
            E
    end.

do_write(M=#mfile{offset=Offset, size=S, file=F, index=Idx},
         DataSize, Metric, Position, Value) when
      is_binary(Metric),
      is_binary(Value) ->

    %% As part of a write operation, both the index and mstore files may be
    %% updated. There is no atomicity gaurantees, and a fault may occur after
    %% one of the writes have been processed. Writing data to the index before
    %% writing to the mstore ensures that offsets are calculated so as not to
    %% cause data to overlap. In other words, data loss is acceptable, but not
    %% data corruption.
    {M1, Base} =
        case btrie:find(Metric, Idx) of
            error ->
                Pos = M#mfile.next,
                Mx = M#mfile{next=Pos+1, index=btrie:store(Metric, Pos, Idx)},
                Bin = <<(byte_size(Metric)):16/integer, Metric/binary>>,
                IdxFile = M#mfile.name ++ ".idx",
                ok = file:write_file(IdxFile, Bin, [read, append]),
                {Mx, Pos*S*DataSize};
            {ok, Pos} ->
                {M, Pos*S*DataSize}
        end,
    P = Base+((Position - Offset)*DataSize),
    R = file:pwrite(F, P, Value),
    {R, M1}.

read_idx(BaseName) ->
    fold_idx(fun({init, Offset, Size}, undefined) ->
                     {Offset, Size, btrie:new(), 0};
                ({entry, M}, {Offset, Size, T, I}) ->
                     {Offset, Size, btrie:store(M, I, T), I+1}
             end, undefined, BaseName).

fold_idx(Fun, Acc0, Chunk, RootName) ->
    FileName = RootName ++ ".idx",
    case file:open(FileName, [read | ?OPTS]) of
        {ok, IO} ->
            case file:read(IO, Chunk) of
                {ok, <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE, R/binary>>} ->
                    Acc = Fun({init, Offset, Size}, Acc0),
                    do_fold_idx(IO, Chunk, Fun, Acc, R);
                _ ->
                    file:close(IO),
                    {error, invalid_file}
            end;
        E ->
            E
    end.

do_fold_idx(IO, Chunk, Fun, AccIn, <<_L:16/integer, M:_L/binary, R/binary>>) ->
    Acc = Fun({entry, M}, AccIn),
    do_fold_idx(IO, Chunk, Fun, Acc, R);
do_fold_idx(IO, Chunk, Fun, AccIn, R) ->
    case file:read(IO, Chunk) of
        {ok, Data} ->
            do_fold_idx(IO, Chunk, Fun, AccIn, <<R/binary, Data/binary>>);
        eof ->
            file:close(IO),
            case R of
                <<>> ->
                    AccIn;
                _ ->
                    {error, invalid_file}
            end;
        E ->
            file:close(IO),
            E
    end.
