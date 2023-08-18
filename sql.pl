:- module(sql, [sqlc_crud//3, create_table//3, sql_table/3]).

sqlc_crud(Name, Cols, Keys) -->
	{ gather_enums(Cols, Enums) },
	create_types(Enums),
	nl_maybe,
	create_table(Name, Cols, Keys),
	nl_maybe,
	sqlc_select_one(Name, Keys),
	nl_maybe,
	sqlc_select_many(Name, Keys),
	nl_maybe,
	sqlc_insert_one(Name, Cols, Keys).

%% create_table(+Name, +Cols, +Keys).
create_table(Name, Cols, Keys) -->
	{ sql_table(Cols, Keys, _TODO) },
	"CREATE TABLE", ws,
	table_name(Name), ws,
	"(", nl_maybe,
	maybe_ws,
	create_table_cols(Cols),
	create_table_keys(Cols, Keys),
	maybe_ws,
	");",
	maybe_ws,
	"\n".

sqlc_select_one(Name, Keys) -->
	sqlc_magic("Get", Name, one),
	"SELECT * FROM ",
	table_name(Name), nl_maybe,
	{ memberchk(primary(PK), Keys) },
	where_eq(PK), ";\n".

sqlc_select_many(Name, Keys) -->
	sqlc_magic("List", Name, many),
	"SELECT * FROM ",
	table_name(Name), nl_maybe,
	{ memberchk(primary(PK), Keys) },
	order_by(PK), ";\n".

sqlc_insert_one(Name, Cols, _Keys) -->
	sqlc_magic("Create", Name, one),
	"INSERT INTO ",
	table_name(Name), ws_maybe,
	"(", nl_maybe,
	comma_list(column_with_placeholder_comment(Cols), Cols),
	")", ws_maybe, "VALUES", ws_maybe, "(", "\n\t",
	placeholders(Cols),
	nl_maybe,
	")", nl_maybe,
	"RETURNING *;\n".

sqlc_magic(Prefix, ModelName, Nature) -->
	"-- name: ",
	seq_not(' ', Prefix),
	title_case(ModelName),
	plural_suffix(ModelName, Nature),
	" ",
	sqlc_nature(Nature),
	nl_maybe.

sqlc_nature(one) --> ":one".
sqlc_nature(many) --> ":many".
sqlc_nature(exec) --> ":exec".

order_by(Key) -->
	"ORDER BY ",
	comma_list(column_name, Key).

where_eq([]) --> [].
where_eq([K|Ks]) -->
	where_eq_([K|Ks], 1).
where_eq_([], _) --> [].
where_eq_([K|Ks], 1) -->
	"WHERE", ws, column_name(K), ws_maybe, "=", ws_maybe, placeholder(1),
	where_eq_(Ks, 2).
where_eq_([K|Ks], N) -->
	{ N > 1 },
	" AND", ws, column_name(K), ws_maybe, "=", ws_maybe, placeholder(N),
	{ succ(N, N2) },
	where_eq_(Ks, N2).

table_name(Name) --> quoted_string(Name).

% column_name(col(Name, _, _)) --> quoted_string(Name).
column_name(Name) --> { Name \= col(_, _, _) }, quoted_string(Name).

column_with_placeholder_comment(Cols, Col) -->
	{ Col = col(Name, _, _) },
	( "\t" | [] ),
	column_name(Name),
	" -- ",
	{ nth1(N, Cols, Col) },
	placeholder(N),
	nl_maybe.

create_types([N-Vs|Cs]) -->
	create_type(N, Vs),
	create_types(Cs).
create_types([]) --> [].

create_type(Name, enum(Vs)) -->
	"CREATE TYPE ",
	quoted_string(Name),
	" AS ENUM",
	"(",
	comma_list(string_literal, Vs),
	");",
	nl_maybe.

create_table_cols(Cols) -->
	maybe_ws,
	comma_list(create_table_col, Cols),
	maybe_ws.

create_table_col(col(Name, Type, Attrs)) -->
	("\t" | []),
	column_name(Name), ws,
	(  { Type = enum(_) }
	-> column_name(Name)
	;  column_type(Type)
	), 
	column_attrs(Attrs),
	"\n".

create_table_keys(_Cols, []) --> [].
create_table_keys(_Cols, [K|Ks]) -->
	maybe_ws,
	",",
	ws_maybe,
	comma_list(create_table_key, [K|Ks]).

create_table_key(primary(Cols)) --> create_table_key_("PRIMARY KEY", Cols).
create_table_key(unique(Cols)) --> create_table_key_("UNIQUE", Cols).

create_table_key_(Kind, Cols) -->
	("\t" | []),
	seq_not('(', Kind), ws,
	"(", maybe_ws,
	key_columns(Cols),
	")", maybe_ws,
	"\n".

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
% Postgres
column_type(serial) --> "SERIAL".
column_type(bigserial) --> "BIGSERIAL".
column_type(smallserial) --> "SMALLSERIAL".
column_type(boolean) --> "BOOLEAN".
column_type(json) --> "JSON".
column_type(xml) --> "XML".
column_type(enum(Values)) --> "ENUM(", comma_list(string_literal, Values), ")".
column_type(interval) --> "INTERVAL".
column_type(array(T)) --> column_type(T), "[]".
column_type(array(T, N)) --> column_type(T), "[", number(N), "]".
column_type(uuid) --> "UUID".
column_type(bytea) --> "BYTEA".
column_type(bit(N)) --> column_type_("BIT", N).
column_type(bit) --> "BIT".
column_type(varbit(N)) --> column_type_("VARBIT", N).
column_type(varbit) --> "VARBIT".
column_type(money) --> "MONEY".
column_type(timestamp_with_timezone) --> "TIMESTAMP", ws, "WITH", ws, "TIME", ws, "ZONE".
column_type(time_with_timezone) --> "TIME", ws, "WITH", ws, "TIME", ws, "ZONE".
column_type(inet) --> "INET".
column_type(cidr) --> "CIDR".
column_type(macaddr) --> "MACADDR".
column_type(point) --> "POINT".
column_type(line) --> "LINE".
column_type(lseg) --> "LSEG".
column_type(box) --> "BOX".
column_type(path) --> "PATH".
column_type(polygon) --> "POLYGON".
column_type(circle) --> "CIRCLE".
column_type(oid) --> "OID".
column_type(literal(Cs)) --> seq_not(' ', Cs).

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

string_literal(Cs) -->
	"'",
	seq_not('\'', Cs),
	"'".

placeholders([]) --> [].
placeholders([_|Xs]) -->
	placeholder(1),
	placeholders(Xs, 2).
placeholders([], _) --> [].
placeholders([_|Rest], N0) -->
	",", ws_maybe,
	placeholder(N0),
	{ succ(N0, N) },
	placeholders(Rest, N).

placeholder(N) --> "$", number(N).

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

% TODO: double check
title_case([C0|Cs]) --> { atom_upper(C0, C) }, [C], title_case_(Cs).
title_case_(['_', C0|Cs]) --> { atom_upper(C0, C) }, [C], title_case_(Cs).
title_case_([C|Cs]) --> { dif(C, '_') }, [C], title_case_(Cs).
title_case_([]) --> [].

plural_suffix(_, many) --> "s".
plural_suffix(_, Nature) --> { dif(Nature, many) }, [].

plural(Cs) --> seq(Cs), "s".

seq_not(_, []) --> [].
seq_not(X, [C|Cs]) -->
	[C],
	{ C \= X },
	seq_not(X, Cs).

ws --> " ".
nl --> "\n" | ws.
nl_maybe --> "\n" | [].
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
% PostgreSQL specific types
type(serial).
type(bigserial).
type(smallserial).
type(boolean).
type(json).
type(enum(Values)) :-
	Values = [_|_],
	maplist(string, Values).
type(interval).
type(array(T)) :- type(T).
type(array(T, N)) :- type(T), integer(N), N > 0.
type(uuid).
type(bytea).
type(bit).
type(bit(N)) :- integer(N).
type(varbit(N)) :- integer(N).
type(varbit).
type(money).
type(timestamp_with_timezone).
type(time_with_timezone).
type(inet).
type(cidr).
type(macaddr).
type(point).
type(line).
type(lseg).
type(box).
type(path).
type(polygon).
type(circle).
type(oid).
type(xml).

attr(null(true)).
attr(null(false)).
attr(unique).
%attr(autoincrement).

gather_enums(Cols, Enums) :-
	setof(Name-enum(Vs), member(col(Name, enum(Vs), _), Cols), Enums).
gather_enums(Cols, []) :-
	\+ member(col(_, enum(_), _), Cols).

