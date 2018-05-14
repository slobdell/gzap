
create_topic:
	gcloud pubsub topics create my-topic

create_subscription:
	gcloud pubsub subscriptions create my-sub --topic my-topic
