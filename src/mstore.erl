%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------

%% @doc An mstore instance is per directory, consisting of multiple `mstore'
%% and `idx' files. Data points are stored according to their timestamp, and
%% stores typically retain up to one week's (depends on `file_size') worth of
%% data. One master `mstore' file will be created for all metric paths in the
%% directory, and is used for coverage queries.
%%
%% Index (`idx') files are keyed on and offset position and consist of metric
%% names.  These metrics are used to calculate the relative position of a
%% metric's values in the `mstore' storage file.
%%
%% The format of an `idx' file is as follows, where `M..' are metric paths:
%% +--------+-----------+----------+----+----------+----+-----+----------+----+
%% | Offset | File Size | Size(M1) | M1 | Size(M2) | M2 | ... | Size(Mn) | Mn |
%% +--------+-----------+----------+----+----------+----+-----+----------+----+
%%                      |---- Pos 0 ----|---- Pos 2 ----|     |---- Pos n ----|
%%
%% Metric names are appended to the index the first time that they are
%% encountered.
%%
%% Storage (mstore) files may store values for more than one metric at a time,
%% and is segmented sequentially.  The position for a value within a
%% segment is calculated as follows: P = Base + ((TS - Offset) * Data size),
%% where Base is a function of the metric's position in the index:
%% +-------------+-----+------------------------------------------------+
%% | Pos(M1) = 0 | --> | Base = 0 X File size X Data size = 0           |
%% | Pos(M2) = 1 | --> | Base = 1 X File size X Data size = 4838400     |
%% | ...         |     |                                                |
%% | Pos(Mn) = N | --> | Base = N X File size X Data size = N * 4838400 |
%% +-------------+-----+------------------------------------------------+
%%
%% Only one process should open the mstore for writing at a given time. At any
%% moment, up to two `mstore' files may be open for writing by the mstore.
%% These files represent the current and previously written to files e.g. the
%% current and previous week.

-module(mstore).

-export([
         new/2,
         open/1,
         close/1,
         delete/1,
         delete/2,
         reindex/1,
         get/4,
         open_mfile/1,
         read_idx/1,
         put/4,
         metrics/1,
         fold/3, fold/4,
         make_splits/3]).
-export_type([mstore/0]).


-include_lib("mmath/include/mmath.hrl").
-include("mstore.hrl").

-define(SIZE_TYPE, unsigned-integer).

-define(VERSION, 3).
-define(OPTS, [raw, binary]).
-define(MFILE_VER, 1).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-record(mfile, {
          name,
          file,
          offset,
          size,
          index = btrie:new(),
          next = 0
         }).

-record(mstore, {
          size,
          files=[],
          dir,
          data_size = ?DATA_SIZE,
          metrics = btrie:new()
         }).

-opaque mstore() :: #mstore{}.

-type fold_fun() :: fun((Metric :: binary(),
                         Offset :: non_neg_integer(),
                         Data :: binary(), AccIn :: any()) -> AccOut :: any()).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Creates a new set or opens an existing one.
%% @end
%%--------------------------------------------------------------------
-spec new(Dir :: string() | binary(),
          Opts :: [{file_size, pos_integer()} |
                   {data_size, pos_integer()}]) ->
                 {ok, mstore()} |
                 {error, filesize_missmatch}.

new(Dir, Opts) when (is_list(Dir) orelse is_binary(Dir)) , is_list(Opts) ->
    {file_size, FileSize} = proplists:lookup(file_size, Opts),
    case proplists:get_value(data_size, Opts) of
        undefined ->
            new(FileSize, ?DATA_SIZE, Dir);
        DataSize ->

            new(FileSize, DataSize, Dir)
    end.

new(FileSize, DataSize, Dir) when
      is_integer(FileSize), FileSize > 0,
      is_binary(Dir) ->
    new(FileSize, DataSize, binary_to_list(Dir));

new(FileSize, DataSize, Dir) ->
    IdxFile = [Dir | "/mstore"],
    case open_mfile(IdxFile) of
        {ok, F, _IS, _Metrics} when F =/= FileSize ->
            {error, filesize_missmatch};
        {ok, _F, IS, _Metrics} when IS =/= DataSize ->
            {error, itemsize_missmatch};
        {ok, F, IS,  Metrics} ->
            {ok, #mstore{size=F, dir=Dir, metrics=Metrics, data_size=IS}};
        _ ->
            file:make_dir(Dir),
            MStore = #mstore{size=FileSize, dir=Dir,
                             data_size=DataSize},
            ok = file:write_file(IdxFile, index_header(MStore)),
            {ok, MStore}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Opens an existing set.
%% @end
%%--------------------------------------------------------------------
-spec open(Dir :: string()) -> {ok, mstore()} | {error, not_found}.

open(Dir) ->
    case open_mfile([Dir | "/mstore"]) of
        {ok, FileSize, DataSize, Metrics} ->
            {ok, #mstore{size=FileSize, dir=Dir, data_size = DataSize,
                         metrics=Metrics}};
        E ->
            E
    end.


%%--------------------------------------------------------------------
%% @doc
%% Closes a metric set.
%% @end
%%--------------------------------------------------------------------
-spec close(mstore()) -> ok.

close(#mstore{files=Files}) ->
    [close_store(S) || {_, S} <- Files],
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Recursively deletes a metric set.
%% @end
%%--------------------------------------------------------------------
-spec delete(mstore()) -> ok | {error, atom()}.

delete(MStore = #mstore{dir=Dir}) ->
    close(MStore),
    {ok, Files} = file:list_dir(Dir),
    Files1 = [[Dir, $/ | File] || File <- Files],
    [file:delete(F) || F <- Files1],
    file:del_dir(Dir).

%%--------------------------------------------------------------------
%% @doc
%% Deletes entries before *Before*, not exact since it's rounded to
%% the next file.
%% @end
%%--------------------------------------------------------------------

-spec delete(mstore(), pos_integer()) -> {ok, mstore()}.
delete(MStore = #mstore{size = S, dir = Dir, files = Files}, Before) ->
    Before1 = ((Before div S) - 1) * S,
    Chunks = chunks(Dir, ".mstore"),
    Chunks1 = lists:takewhile(fun(C) ->
                                      C < Before1
                              end, Chunks),
    case Chunks1 of
        [] ->
            {ok, MStore};
        _ ->
            [close_store(F) || {_, F} <- Files],
            [begin
                 F = [Dir, $/, integer_to_list(C), $.],
                 file:delete([F | "mstore"]),
                 file:delete([F | "idx"])
             end || C <- Chunks1],
            reindex(MStore#mstore{files = []})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads data from a set.
%% @end
%%--------------------------------------------------------------------
-spec get(
        mstore(),
        binary(),
        non_neg_integer(),
        pos_integer()) ->
                 {'error',atom()} |
                 {ok, binary()}.

get(#mstore{size=S, files=FS, dir=Dir, data_size=DataSize},
    Metric, Time, Count) when
      is_binary(Metric),
      is_integer(Time), Time >= 0,
      is_integer(Count), Count > 0 ->
    ?DT_READ_ENTRY(Metric, Time, Count),
    Parts = make_splits(Time, Count, S),
    R = do_get(S, FS, Dir, DataSize, Metric, Parts, <<>>),
    ?DT_READ_RETURN,
    R.

%%--------------------------------------------------------------------
%% @doc
%% Folds over all the metrics in a store. Please be aware that the
%% data for a single metric is not guaranteed to be delivered at a time
%% it is split on non set values and file boundaries!
%% @end
%%--------------------------------------------------------------------

-spec fold(mstore(), FoldFun :: fold_fun(), AccIn :: any()) ->
                  AccOut :: any().
fold(MStore, FoldFun, Acc) ->
    fold(MStore, FoldFun, infinity, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link: fold/3} but data is chunked at a maximum size of
%% *ChunkSize*.
%% @end
%%--------------------------------------------------------------------

-spec fold(mstore(), FoldFun :: fold_fun(), ChunkSize :: pos_integer() | infinity,
           AccIn :: any()) ->
                  AccOut :: any().
fold(#mstore{dir=Dir, data_size = DataSize}, Fun, Chunk, Acc) ->
    serialize_dir(Dir, DataSize, Fun, Chunk, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Writes data into a mstore.
%% @end
%%--------------------------------------------------------------------
-spec put(mstore(), binary(), non_neg_integer(),
          integer() | [integer()] | binary()) ->
                 mstore().

put(MStore, Metric, Time, [V0 | _] = Values)
  when is_integer(V0) ->
    put(MStore, Metric, Time, mmath_bin:from_list(Values));

put(MStore, Metric, Time, V) when is_integer(V) ->
    put(MStore, Metric, Time, mmath_bin:from_list([V]));

put(MStore = #mstore{size=S, files=CurFiles, metrics=Ms, data_size = DataSize},
    Metric, Time, Value)
  when is_binary(Value),
       is_integer(Time), Time >= 0,
       is_binary(Metric) ->
    ?DT_WRITE_ENTRY(Metric, Time, mmath_bin:length(Value)),
    Count = mmath_bin:length(Value),
    Parts = make_splits(Time, Count, S),
    Parts1 = [{B, round(C * DataSize)} || {B, C} <- Parts],
    MStore1 = case btrie:is_key(Metric, Ms) of
                  true ->
                      MStore;
                  false ->
                      MStorex = MStore#mstore{metrics=btrie:store(Metric, Ms)},
                      ok = file:write_file([MStorex#mstore.dir | "/mstore"],
                                      <<(byte_size(Metric)):16/integer, Metric/binary>>,
                                      [read, append]),
                      MStorex
              end,
    CurFiles1 = do_put(MStore1, Metric, Parts1, Value, CurFiles),
    ?DT_WRITE_RETURN,
    MStore1#mstore{files = CurFiles1}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a set of all metrics in the store.
%% @end
%%--------------------------------------------------------------------

-spec metrics(mstore()) -> btrie:btrie().

metrics(#mstore{metrics=M}) ->
    M.


%%--------------------------------------------------------------------
%% @doc
%% Utility function to split a continous data streams in chunks for a
%% given time range size.
%%
%% * Time - the offset of the first value.
%% * Count - the total number of values.
%% * Size - The size of the chunking.
%% @end
%%--------------------------------------------------------------------
-spec make_splits(Time :: non_neg_integer(),
                  Count :: pos_integer(),
                  Size :: pos_integer()) ->
                         [{StartTime :: non_neg_integer(),
                           ChunkSize :: pos_integer()}].

make_splits(Time, Count, Size) ->
    make_splits(Time, Count, Size, []).

-spec reindex(mstore()) -> {ok, mstore()}.

reindex(MStore = #mstore{dir = Dir}) ->
    IdxFileOld = [Dir | "/mstore"],
    IdxFileNew = [Dir | "/mstore.new"],

    Files = filelib:wildcard([Dir | "/*.idx"]),
    {ok, IO} = file:open(IdxFileNew, [write | ?OPTS]),
    ok = file:write(IO, index_header(MStore)),
    Metrics = lists:foldl(fun (F, Set) ->
                                  reindex_chunk(IO, F, Set)
                          end, btrie:new(), Files),
    ok = file:close(IO),
    NewIndex = << <<(byte_size(Metric)):16/integer, Metric/binary>>
                  || Metric <- btrie:fetch_keys(Metrics) >>,
    ok = file:write_file(IdxFileNew, <<(index_header(MStore))/binary,
                                       NewIndex/binary>>),
    ok = file:delete(IdxFileOld),
    ok = file:rename(IdxFileNew, IdxFileOld),
    {ok, MStore#mstore{metrics = Metrics}}.

%%====================================================================
%% Private functions.
%%====================================================================

chunks(Dir, Ext) ->
    lists:sort([list_to_integer(filename:rootname(filename:basename(F))) ||
                   F <- filelib:wildcard([Dir | "/*" ++ Ext])]).

reindex_chunk(IO, File, Set) ->
    fold_idx(fun({entry, M}, Acc) ->
                     ok = file:write(IO, <<(byte_size(M)):16/integer, M/binary>>),
                     btrie:store(M, Acc);
                (_, Acc) ->
                     Acc
             end, Set, File).

index_header(#mstore{size=FileSize, data_size=DataSize}) ->
    <<?VERSION:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE, DataSize:64/?SIZE_TYPE>>.

make_splits(_Time, 0, _Size, Acc) ->
    lists:reverse(Acc);

make_splits(Time, Count, Size, Acc) ->
    Base = (Time div Size)*Size,
    case Time - Base of
        D when (D + Count) < Size ->
            lists:reverse([{Time, Count} | Acc]);
        D ->
            Inc = Size-D,
            make_splits(Time + Inc, Count - Inc, Size, [{Time, Inc} | Acc])
    end.

store_metrics(#mfile{index=M}) ->
    btrie:fetch_keys(M).

open_mfile(F) ->
    Chunk = 4*1024,
    case file:open(F, [read | ?OPTS]) of
        {ok, IO} ->
            case file:read(IO, Chunk) of
                {ok, <<2:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE, R/binary>>} ->
                    Set = do_fold_idx(IO, Chunk,
                                     fun({entry, M}, Acc) ->
                                             btrie:store(M, Acc)
                                     end, btrie:new(), R),
                    {ok, FileSize, 8, Set};
                {ok, <<?VERSION:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE,
                       DataSize:64/?SIZE_TYPE, R/binary>>} ->
                    Set = do_fold_idx(IO, Chunk,
                                     fun({entry, M}, Acc) ->
                                             btrie:store(M, Acc)
                                     end, btrie:new(), R),
                    {ok, FileSize, DataSize, Set};
                {ok, _} ->
                    file:close(IO),
                    {error, invalid_file};
                _ ->
                    file:close(IO),
                    {error, invalid_file}
            end;
        E ->
            E
    end.

do_put(_, _, [], <<>>, Files) ->
    Files;

do_put(MStore = #mstore{size=S, data_size = DataSize}, Metric,
       [{Time, Size} | R], InData,
       [{FileBase, F} | FileRest]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = write(F, DataSize, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F1} | FileRest]);

do_put(MStore = #mstore{size=S, data_size = DataSize}, Metric,
       [{Time, Size} | R], InData,
       [First, {FileBase, F}]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = write(F, DataSize, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F1}, First]);

do_put(MStore = #mstore{size=S, dir=D, data_size = DataSize}, Metric,
       [{Time, Size} | R], InData,
       [First, {_Other, F}]) ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    close_store(F),
    Base = [D, $/, integer_to_list(FileBase)],
    {ok, F1} = open(Base, FileBase, S, write),
    {ok, F2} = write(F1, DataSize, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F2}, First]);

do_put(MStore = #mstore{size=S, dir=D, data_size = DataSize}, Metric,
       [{Time, Size} | R], InData,
       Files) when length(Files) < 2 ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    Base = [D, $/, integer_to_list(FileBase)],
    {ok, F1} = open(Base, FileBase, S, write),
    {ok, F2} = write(F1, DataSize, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F2} | Files]).

do_get(_, _, _, _, _, [], Acc) ->
    {ok, Acc};


do_get(S, [{FileBase, F} | _] = FS,
       Dir, DataSize, Metric, [{Time, Count} | R], Acc)
  when ((Time div S)*S) =:= FileBase ->
    case read(F, DataSize, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc2);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        E ->
            E
    end;

do_get(S,
       [_, {FileBase, F}] = FS,
       Dir, DataSize, Metric, [{Time, Count} | R], Acc)
  when ((Time div S)*S) =:= FileBase ->
    case read(F, DataSize, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc2);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        E ->
            E
    end;

do_get(S, FS, Dir, DataSize, Metric, [{Time, Count} | R], Acc) ->
    FileBase = (Time div S)*S,
    Base = [Dir, "/", integer_to_list(FileBase)],
    {ok, F} = open(Base, FileBase, S, read),
    case read(F, DataSize, Metric, Time, Count) of
        {ok, D} ->
            close_store(F),
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc2);
        {error,not_found} ->
            close_store(F),
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        eof ->
            close_store(F),
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, DataSize, Metric, R, Acc1);
        E ->
            close_store(F),
            E
    end.

close_store(#mfile{file=F}) ->
    file:close(F).

open(File, Offset, Size, Mode) ->
    FileOpts = case Mode of
                   read ->
                       [read | ?OPTS];
                   write ->
                       [read, write | ?OPTS]
               end,
    IdxFile = [File | ".idx"],
    case read_idx(IdxFile) of
        {O, S, Idx, Next} when Offset =:= O,
                         Size =:= S ->
            case file:open([File | ".mstore"], FileOpts) of
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
            case file:open([File |".mstore"], [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mfile{name=File, file=F, offset=Offset,
                               size=Size, next=0},
                    file:write_file(IdxFile,
                                    <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE>>),
                    {ok, M};
                E ->
                    E
            end
    end.

open_store(File) ->
    case read_idx([File | ".idx"]) of
        {Offset, Size, Idx, Next} ->
            case file:open([File | ".mstore"], [read | ?OPTS]) of
                {ok, F} ->
                    {ok, #mfile{index=Idx, name=File, file=F, offset=Offset,
                                size=Size, next=Next}};
                E ->
                    E
            end;
        E ->
            E
    end.

write(M=#mfile{offset=Offset, size=S},
      DataSize, Metric, Position, Value)
  when is_binary(Value),
       Position >= Offset,
       (Position - Offset) + (byte_size(Value) div DataSize) =< S ->
    do_write(M, DataSize, Metric, Position, Value).

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
                ok = file:write_file([M#mfile.name | ".idx"], Bin,
                                     [read, append]),
                {Mx, Pos*S*DataSize};
            {ok, Pos} ->
                {M, Pos*S*DataSize}
        end,
    P = Base+((Position - Offset)*DataSize),
    R = file:pwrite(F, P, Value),
    {R, M1}.

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

serialize_dir(Dir, DataSize, Fun, Chunk, Acc) ->
    {ok, Fs} = file:list_dir(Dir),
    Fs1 = [re:split(F, "\\.", [{return, list}]) || F <- Fs],
    Idxs = [I || [I, "idx"] <- Fs1],
    lists:foldl(fun(I, AccIn) ->
                        serialize_index([Dir, $/, I], DataSize, Fun, Chunk, AccIn)
                end, Acc, Idxs).

serialize_index(Store, DataSize, Fun, Chunk, Acc) ->
    {ok, MFile} = open_store(Store),
    Res = lists:foldl(fun(M, AccIn) ->
                              serialize_metric(MFile, DataSize, M, Fun, Chunk, AccIn)
                      end, Acc, store_metrics(MFile)),
    close_store(MFile),
    Res.


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

read_idx(F) ->
    fold_idx(fun({init, Offset, Size}, undefined) ->
                     {Offset, Size, btrie:new(), 0};
                ({entry, M}, {Offset, Size, T, I}) ->
                     {Offset, Size, btrie:store(M, I, T), I+1}
             end, undefined, F).
fold_idx(Fun, Acc0, F) ->
    fold_idx(Fun, Acc0, 4*1024, F).

fold_idx(Fun, Acc0, Chunk, F) ->
    case file:open(F, [read | ?OPTS]) of
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
