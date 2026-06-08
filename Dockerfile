# This docker file is just to run the rabbitmq service with consistent hashing plugin installed 

# Use the management image so you still get the UI
FROM rabbitmq:3-management

# Enable the consistent hash plugin offline during the build
RUN rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange
