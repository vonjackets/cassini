
client connects, listener created, 


client registers

session is created

client sends request to subscribe to session

session forwards req to the broker

broker forwards to sub mgr AND topic mgr

sub mgr creates new subscriber, topic mgr creates new topic - does nothing

sub mgr sends ack to broker

broker forwards ack to session

PUBLISH

client connects, listener created, 

client registers

session is created

client requests publish 
session forwards request to broker
    WHAT SHOULD HAPPEN
        Some auth logic

broker forwards to topicmgr
(Not a great idea) - creates a bottleneck, we can just forward directly to the topic actor IF it exists

    WHAT SHOULD HAPPEN
    IF NOT, we should tell the topicmgr to create one, AND THEN publish that message. (Consider abuse case of someone sending a bunch of publishes with random topic names)


topicmgr finds the topic - creates an actor if it can't
forwards the request if it does


topic actor gets publish request, adds to queue
topic actor sends ack to topicmgr
topicmgr sends to broker
broker sends to session





We're getting rid of the subscriber actors and its manager,

## Subscription flow

client connects, listener created, 

client registers

session is created

client sends request to subscribe to session

session forwards to broker
broker performs auth
if auth successful - broker looks for existing subscriber actor.
    If no subscriber exists, forward subscribe request topic IF it exists
        IF NOT, we should tell the topicmgr to create one, AND THEN subscribe by appending the subscriber_id ("session_id:topic") to the topic agent's list. (Consider abuse case of someone sending a bunch of publishes with random topic names)
   
        Forward subscribe request to sub mgr letting
        sub mgr creates subscriber actor
        new subscriber actor sends ACK directly to session with success

    IF subscriber exists, send ACK back to session with success


    session sends ACK to client via the listener IF it exists
    
    if not, log error and wait around to die OR for the listener to come back

ELSE, send 403 error


NOTE: if a subscriber *fails* i.e. panics, errors out, etc. Inform client to resend subscriptions
Our only other recourse is to just kill the session entirely...not ideal



NEW Publish flow

client connects, listener created, 


client registers

session is created

client sends request to subscribe to session

session forwards to broker
broker performs auth
if auth successful - broker looks for existing topic actor.

    IF topic exists, 
        sends publish request to topic
        If any, topic actor forwards notifications to subscribers 


    If no topic exists, EXPECT to forward  publish request topicMgr,
        topic mgr starts new topic, sends publish request to topic
        topic actor adds message to queue
        
        If any, topic actor forwards  notifications to subscribers,

    
    Topic directly informs session that sent the initial request via an ACK

    subscribers forward notificiation to sessions
    sessions forward to client via listener    
ELSE, send 403 error


NOTE: We can use RPC to wait for a reply from subscribers and block while we wait for a reply,
If we timeout on any particular send, we can put the message back onto the queue 

NOTE: if a subscriber *fails* i.e. panics, errors out, etc. Inform client to resend subscriptions
Our only other recourse is to just kill the session entirely...not ideal


