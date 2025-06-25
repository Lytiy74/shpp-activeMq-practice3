### Practical #3 – ActiveMQ Message Stream Validator (Difficulty: 5 s.p.)

* Stream-generates `N` random POJOs (with name, eddr, count, date).
* Sends to ActiveMQ queue (configurable via properties).
* Generation stops after time defined in `.properties` (via poison pill).
* Messages consumed in parallel, validated:

  * `name` length ≥ 7, contains 'a'
  * `count` ≥ 10
  * Valid EDDR number
* Valid messages → `valid.csv`; invalid → `invalid.csv` with error JSON.
* Full Jenkins integration with testing and performance benchmark (≥ 3k msg/sec).
