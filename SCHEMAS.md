# Schemas

This application uses Redis as a backend store with Pydantic for model validation. In order to faciliate updates to the Pydantic
models in a graceful way, there is a schema system. This system relies on SHA256 sums of each model in
[`shared_types.py`](/inky-mbta-tracker/shared_types/shared_types.py) & [`mbta_responses.py`](/inky-mbta-tracker/mbta_responses.py).

These calculations are stored in [`class_hashes.py`](/inky-mbta-tracker/shared_types/class_hashes.py) and calculated using the CLI
command `task compute-class-hashes`. These hashes are compared against hashes stored in Redis for several categories of schema
outlined in [`schema_versioner.py`](/inky-mbta-tracker/shared_types/schema_versioner.py). If these hashes don't match up for a
particular schema category, all the keys associated with that schema in the Redis store will be deleted. The application is architected
to withstand such deletions gracefully by recreating the data through API calls. It's up to the developer to run the class
computation step whenever they update the associated model files as this validation step occurs on startup.
