if exists("b:current_syntax")
  finish
endif

syntax case ignore
syntax keyword aqlKeyword FOR LET FILTER SEARCH SORT LIMIT COLLECT AGGREGATE RETURN DISTINCT WINDOW WITH INTO KEEP COUNT OPTIONS PRUNE GRAPH SHORTEST_PATH K_SHORTEST_PATH K_PATHS ALL_SHORTEST_PATHS OUTBOUND INBOUND ANY
syntax keyword aqlModification INSERT UPDATE REPLACE REMOVE UPSERT
syntax keyword aqlBoolean true false null
syntax keyword aqlOperator AND OR NOT IN LIKE ALL NONE ANY AT LEAST
syntax match aqlBindVar /@@\=[A-Za-z_][A-Za-z0-9_]*/
syntax match aqlNumber /\<\d\+\(\.\d\+\)\=\>/
syntax match aqlLineComment /\/\/.*$/
syntax region aqlBlockComment start=/\/\*/ end=/\*\// contains=aqlTodo
syntax keyword aqlTodo TODO FIXME NOTE contained
syntax region aqlString start=/"/ skip=/\\./ end=/"/
syntax region aqlString start=/'/ skip=/\\./ end=/'/
syntax region aqlIdentifier start=/`/ skip=/\\./ end=/`/

highlight default link aqlKeyword Keyword
highlight default link aqlModification Statement
highlight default link aqlBoolean Boolean
highlight default link aqlOperator Operator
highlight default link aqlBindVar Identifier
highlight default link aqlNumber Number
highlight default link aqlLineComment Comment
highlight default link aqlBlockComment Comment
highlight default link aqlTodo Todo
highlight default link aqlString String
highlight default link aqlIdentifier Identifier

let b:current_syntax = "aql"
