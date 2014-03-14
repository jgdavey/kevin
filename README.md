Kevin Bacon's favourite reposotory
==================================

How far is an actor from Kevin Bacon?

## Setup

1. Make sure leiningen is installed
2. `brew install datomic`
3. Copy .lein-env.example to .lein-env
4. Copy dev/transactor.example.properties to dev/transactor.properties
5. In another pane, run `datomic-transactor $PWD/dev/transactor.properties`
6. Start a repl with `lein repl`
7. Within the repl, run `(go)`

You can now visit http://localhost:3000 to see the web app. You also now have an
empty datomic database.

## Import

1. At a REPL, after you've run `(go)`, run the following:

        (import-sample-data)
