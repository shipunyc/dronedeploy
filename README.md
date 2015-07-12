## Installation

pip install -r requirements


## Message Protocol

drone: sends its own id, receives 'DROP' or 'OK+[handler id]'

handler: sends its own id, receives 'FREE' or 'OK+[drone id]'

manager: receives drone ids and handler ids, sends 'FREE' or 'OK+[done id]' to
handlers, and sends 'DROP' or 'OK+[handler id]' to drones.


## Test

Start manager with: python manager.py --listen\_drones=127.0.0.1:5551 --listen\_handlers=127.0.0.1:5552

Start a handler with: python handler.py --manager=127.0.0.1:5552 --id=[any id]

Start a drone with: python drone.py --manager=127.0.0.1:5551 --id=[any id]

Start as many handler or drones as you wish, then try stoping some of them with CRTL + C.
