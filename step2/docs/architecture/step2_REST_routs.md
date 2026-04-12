## This is a description of the design of the REST server providing access to STEP2 databases via the DML commands.

The REST API is implemented using Chi router following the examples at https://github.com/go-chi/chi/tree/master/_examples/rest.

The routs are divided into groups, indicated by the second element of the route
- `DB` group
- `record` group
- `set group`, and
- `management` group
- `batch` group

The record group has a subgroup for different forms of `Get` commands. The `BD` group has a subgroup for getting DB schema and storage information.

The routs map into the STEP commands as follows

step/db/create -> CreateDB_DML
step/db/open -> OpenDB_DML
step/db/close -> CloseDB_DML
step/db/info/schema -> GetSchema_DML
step/db/info/tables -> GetTableStats

step/record/add -> AddNewRecord_DML
step/record/update -> UpdateRecord_DML
step/record/delete -> DeleteRecord_DML

step/record/get/id -> GetRecordID_DML
step/record/get/next -> GetNextRecord_DML
step/record/get/byid - GetRecordByID_DML
step/record/get/bykey -> GetRecordByKey_DML
step/record/get/bystring -> GetRecordsBySting_DML
step/record/get/bysubstring -> GetRecordsBySubstring_DML

step/set/add -> AddSetMember_DML
step/set/get -> GetSetMember_DML
step/set/remove -> RemoveSetMember_DML

step/batch

- If a partial rout ends with "/" and not arguments, like step/db/, it is a request for description of the next element(s) of the rout so
+ step/db/ should return information that the six options: create, open, close, info/schema, info/table and info/stats

- If a complete route, e.g. step/db/open/ ends with "/" the server should return short description of the OpenDB command, the JSON object that should be passed to it, and what will be returned (as it is described in the command documentation)

