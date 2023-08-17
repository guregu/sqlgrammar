% :- module(sql, []).


test :-
	once(phrase(create_table("test", [
		col("a", varchar(255), []),
		col("foo", bigint, []),
		col("bar", decimal, [null(true)]),
		col("baz", decimal(10, 2), [])
	], [
		primary(["a", "foo"]),
		unique(["baz"])
	]), Cs)),
	format("~n~s~n~n", [Cs]).

%% create_table(+Name, +Cols, +Keys).
create_table(Name, Cols, Keys) -->
	{ sql_table(Cols, Keys, _TODO) },
	"CREATE TABLE", ws,
	table_name(Name), ws,
	"(",
	maybe_ws,
	create_table_cols(Cols),
	create_table_keys(Cols, Keys),
	maybe_ws,
	")",
	maybe_ws.

table_name(Name) --> quoted_string(Name).

column_name(Name) --> quoted_string(Name).

create_table_cols(Cols) -->
	maybe_ws,
	comma_list(create_table_col, Cols),
	maybe_ws.

create_table_col(col(Name, Type, Attrs)) -->
	column_name(Name), ws,
	column_type(Type), 
	column_attrs(Attrs).

create_table_keys(_Cols, []) --> [].
create_table_keys(_Cols, [K|Ks]) -->
	maybe_ws,
	",",
	ws_maybe,
	comma_list(create_table_key, [K|Ks]).

create_table_key(primary(Cols)) --> create_table_key_("PRIMARY KEY", Cols).
create_table_key(unique(Cols)) --> create_table_key_("UNIQUE", Cols).

create_table_key_(Kind, Cols) -->
	seq_not('(', Kind), ws,
	"(", maybe_ws,
	key_columns(Cols),
	")", maybe_ws.

column_type(char(N)) --> column_type_("CHAR", N).
column_type(char) --> "CHAR".
column_type(varchar(N)) --> column_type_("VARCHAR", N).
column_type(varchar) --> "VARCHAR".
column_type(text) --> "TEXT".
column_type(nchar(N)) --> column_type_("NCHAR", N).
column_type(nchar) --> "NCHAR".
column_type(nvarchar(N)) --> column_type_("NVARCHAR", N).
column_type(nvarchar) --> "NVARCHAR".
column_type(binary(N)) --> column_type_("BINARY", N).
column_type(binary) --> "BINARY".
column_type(varbinary(N)) --> column_type_("VARBINARY", N).
column_type(varbinary) --> "VARBINARY".
column_type(float) --> "FLOAT".
column_type(real) --> "REAL".
column_type(double_precision) --> "DOUBLE", ws, "PRECISION".
column_type(decimal(P, S)) --> column_type_("DECIMAL", P, S).
column_type(decimal(P)) --> column_type_("DECIMAL", P).
column_type(decimal) --> "DECIMAL".
column_type(numeric(P, S)) --> column_type_("NUMERIC", P, S).
column_type(numeric(P)) --> column_type_("NUMERIC", P).
column_type(numeric) --> "NUMERIC".
column_type(smallint) --> "SMALLINT".
column_type(integer) --> "INTEGER".
column_type(bigint) --> "BIGINT".
column_type(date) --> "DATE".
column_type(time) --> "TIME".
column_type(timestamp) --> "TIMESTAMP".

column_type_(Type, N) -->
	seq_not('(', Type),
	maybe_ws,
	"(",
	maybe_ws,
	number(N),
	maybe_ws,
	")".

column_type_(Type, N1, N2) -->
	seq(Type),
	maybe_ws,
	"(",
	maybe_ws,
	number(N1),
	maybe_ws,
	",",
	maybe_ws,
	number(N2),
	maybe_ws,
	")".

column_attrs([]) --> [].
column_attrs([A|As]) -->
	ws,
	column_attr(A),
	column_attrs(As).
% column_attrs_([]) --> [].
% column_attrs_([A|As]) -->
% 	ws,
% 	column_attr(A),
% 	column_attrs_(As).

column_attr(null(false)) --> "NOT NULL".
column_attr(null(true)) --> "NULL".
column_attr(unique) --> "UNIQUE". % TODO: check

key_columns(Cols) -->
	comma_list(column_name, Cols).

quoted_string(Name) -->
	% TODO: most compatible quoting mechanism?
	"\"",
	seq_not('"', Name),
	"\"".

number(N) -->
	{ number_chars(N, Cs) },
	seq(Cs).

comma_list(_, []) --> [].
comma_list(Nonterminal, [X|Xs]) -->
	call(Nonterminal, X),
	comma_list_(Nonterminal, Xs).
comma_list_(_, []) --> [].
comma_list_(Nonterminal, [X|Xs]) -->
	",",
	ws_maybe,
	call(Nonterminal, X),
	comma_list_(Nonterminal, Xs).

seq_not(_, []) --> [].
seq_not(X, [C|Cs]) -->
	[C],
	{ C \= X },
	seq_not(X, Cs).

ws --> " ".
nl --> "\n" | ws.
ws_maybe --> ws | [].
maybe_ws --> [] | ws.

sql_table(Cols, Keys0, _Opts) :-
	columns(Cols),
	sort(Keys0, Keys),
	keys(Cols, Keys),
	% TODO: opts
	true.

keys(Cols, [Key|Ks]) :-
	key(Key),
	key_columns(Key, KeyCols),
	maplist(key_exists_(Cols, Key), KeyCols),
	keys(Cols, Ks).
keys(_, []).

key_exists_(Cols, Key, KeyCol) :-
	(  memberchk(col(KeyCol, _, _), Cols)
	-> true
	;  throw(error(sql_error(unknown_column, KeyCol, Key), sql_table/3))
	).

primary_key(Ks, PK) :-
	select(key(primary(PK)), Ks, Ks0),
	% dif(PK, Other),
	(  memberchk(key(primary(Other)), Ks0)
	-> throw(error(sql_error(pk_conflict, PK, Other)))
	;  true
	).

key(primary([_|_])).
key(unique([_|_])).
key(foreign([_|_], _Table, [_|_])).

key_columns(primary(Cs), Cs).
key_columns(unique(Cs), Cs).
key_columns(foreign(Cs1, _, Cs2), Cs) :- append(Cs1, Cs2, Cs).

columns([col(N, T, A)|Cs]) :-
	col(N, T, A),
	columns(Cs).
columns([]).

col(Name, Type, Attrs) :-
	string(Name),
	(  type(Type)
	-> true
	;  throw(error(sql_error(unknown_type, Type, col(Name, Type)), col/3))
	),
	maplist(attr, Attrs).

type(char(N)) :- integer(N).
type(char).
type(varchar(N)) :- integer(N).
type(varchar).
type(text).
type(nchar(N)) :- integer(N).
type(nchar).
type(nvarchar(N)) :- integer(N).
type(nvarchar).
type(binary(N)) :- integer(N).
type(binary).
type(varbinary(N)) :- integer(N).
type(varbinary).
type(float).
type(real).
type(double_precision).
type(decimal(P, S)) :- integer(P), integer(S).
type(decimal(P)) :- integer(P).
type(decimal).
type(numeric(P, S)) :- integer(P), integer(S).
type(numeric(P)) :- integer(P).
type(numeric).
type(smallint).
type(integer).
type(bigint).
type(date).
type(time).
type(timestamp).

attr(null(true)).
attr(null(false)).
attr(unique).
%attr(autoincrement).
