import os

import newrelic.agent

# Initialize newrelic agent before importing anything else
# The newrelic config file is hardcoded because we can't import nothing before this :-(
newrelic.agent.initialize("{}/newrelic.ini".format(os.getcwd()))  # noqa


from app import create_app

app = create_app()
