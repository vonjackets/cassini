# Client Registration

client connects

listener manager creates new listener

listener waits for RegistrationRequest

client sends RegistrationRequest to listener over tcp

if registration id is missing OR incorrect (doesn't match any existing session)

    listener sends error mesasge back to client

else if id is missing,

    listener forwards request to register to broker supervisor to authenticate user

    if auth successful
        broker forwards the RegistrationRequest to the session mgr
        session mgr creates new session with a new session id
        session actor starts and sends a RegistrationResponse to the listener containing its session id
        listener updates its state with the session id and forwards response back to client
        client receives request and updates its state with session id

    else 
        broker sends listener back RegistrationResponse with an error

else if client provided some valid session_id
    Listener forwards RegistrationRequest to the session
    session actor updates state with new client id, sends a RegistrationResponse to the listener, then finally informs the sessionManager to stop it from reclaiming the session
    session manager stops cleanup timer
    listener forwards registrationResponse to the client




# Proposed Registration Flow

We don't have a way for subscribers to empty their DLQs
what if we did this flow?


client connects

listener manager creates new listener

listener waits for RegistrationRequest

client sends RegistrationRequest, optionally with a session id to listener over tcp

listener forwards request to broker to perform auth
if auth successful
        
    Broker tries to lookup existing session using provided session_id
    If no session id provided,

        broker forwards the RegistrationRequest to the session mgr
        session mgr creates new session with a new session id
        session actor starts and sends a RegistrationResponse to the listener containing its session id
        listener updates its state with the session id and forwards response back to client
        client receives request and updates its state with session id

    else if client provided some valid session_id
        Broker forwards RegistrationRequest to the session AND to subscriber mgr
        subscriber mgr finds all subscribers for that session and forwards the registrationRequest to them
        each subscriber empties it's dead letter queues, sending the contained messages to the session
        session actor updates state with new client id, sends a RegistrationResponse to the listener, then finally informs the sessionManager to stop it from reclaiming the session
        session manager stops cleanup timer
        listener forwards registrationResponse to the client



