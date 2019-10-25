## PipeDrive Deal Workflow Automation

This is an AWS SAM project that exposes an API Gateway endpoint to accept payloads from PipeDrive's webhooks. These webhooks trigger anytime a deal is added to PipeDrive or a deal moves from one stage to the next

### Automated Flows

The following items have been automated.

* GDrive
  *
* Slack
  *

See https://github.com/stelligent/pipedrive-automation/issues for a list of future plans

### Code Layout

This microservice follows a cell-based architecture. A cell is made up of one or more Components.

The template.yaml that resides in the root directory describes the entrypoint into the microservice.
For this microservice, an API Gateway is created which is used to accept payloads from PipeDrive's webhooks.
The event is then passed on to the pipedrive component where it is processed.

In the Components directory, you will find the following components:

* pipedrive - filters webhook events and orchestrates the PipeDrive workflow.
* gdrive - creates the GDrive folder structure when a new deal is added
* slack - handles all interactions with Slack

### Endpoints

TODO

### Deployment

TODO

#### Testing

TODO

### Webhook Configuration

TODO

#### Monitored fields

TODO

#### POST'ed fields

TODO

### Useful Links

TODO
