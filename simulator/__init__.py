"""Payment-provider simulator for Hookgate (FR-SIM-1..3, SDD 15.1).

It plays the provider: builds a deterministic delivery plan from a scenario and a seed,
signs each delivery at send time, sends it over HTTP, and then checks the outcome with
an expected-state oracle through the admin API. It does not import ``hookgate``: it
treats the system as a black box, like a real provider would, so a bug in Hookgate
cannot hide behind code shared with the oracle.
"""
