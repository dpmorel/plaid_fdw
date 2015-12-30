from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from requests import get
from requests import post
import logging

class DnbAuthForeignDataWrapper(ForeignDataWrapper):

  def __init__(self, options, columns):
      super(DnbAuthForeignDataWrapper, self).__init__(options, columns)
      self.validate(options, columns)

      self.columns = columns
      self.x_dnb_user = options['x_dnb_user']
      self.x_dnb_pwd = options['x_dnb_pwd']
      self.uri = options['uri']

  def validate(self, options, columns):
      if 'uri' not in options:
          log(message = 'No uri given', level = logging.ERROR)


  def handle_error(self, response):
      error = response['error']['message']
      log(message = error, level = logging.ERROR)


  def execute(self, quals, columns):
      headers = {'x-dnb-user': self.x_dnb_user, 'x_dnb_pwd':self.x_dnb_pwd }
      request = post('https://maxcvservices.dnb.com/Authentication/V2.0',headers=headers)
      response = request.json()
      if 'error' in response:
          self.handle_error(response)
      token = response['AuthenticationDetail']['Token']

      headers = {'Authorization', token}
      request = get(self.uri, headers=headers)

      if 'error' in response:
          self.handle_error(response)
      else:
          yield response['MatchResponse']['MatchResponseDetail']['MatchCandidate']
