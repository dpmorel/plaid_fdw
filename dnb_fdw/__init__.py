
from collections import OrderedDict
from json import dumps
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from multicorn.utils import ERROR, WARNING, DEBUG
from requests import get
from requests import post

class DnbForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(DnbForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        try:
          self.x_dnb_user = options['x_dnb_user']
          self.x_dnb_pwd = options['x_dnb_pwd']
          self.type = options['type']
        except KeyError:
            log(message = 'You must pass an x_dnb_user, x_dnb_pwd and uri', level = ERROR)

    def get_token(self):
        headers = {
            'x-dnb-user': self.x_dnb_user,
            'x-dnb-pwd': self.x_dnb_pwd
        }
        request = post('https://maxcvservices.dnb.com/Authentication/V2.0',headers = headers)
        response = request.json()
        if 'error' in response:
            log(message = 'error happened processing auth', level = ERROR)
        try:
            return response['AuthenticationDetail']['Token']
        except KeyError:
            log(message = 'Could not find AuthenticationDetail or Token in response', level = ERROR)

    def query(self, quals, columns):
        token = self.get_token();
        uri = 'https://maxcvservices.dnb.com/V5.0/organizations?match=true&MatchTypeText=Basic&CountryISOAlpha2Code=US'
        for q in quals:
            uri += '&' + q.field_name + '=' + q.value

        headers = {'Authorization': token}
        request = get(uri, headers=headers)
        response = request.json()
        if 'error' in response:
            log(message = 'error happened processing query', level = ERROR)
        else:
            for candidate in response['MatchResponse']['MatchResponseDetail']['MatchCandidate']:
                row = OrderedDict()
                for col in columns:
                    for q in quals:
                        if col == q.field_name:
                            row[col] = q.value;
                row['candidate'] = dumps(candidate)
                yield row

    def details(self, quals, columns):
        token = self.get_token();
        for q in quals:
            if q.field_name == 'duns':
                duns = q.value
        if duns is None:
            log(message = 'You must pass a duns number', level = ERROR)

        uri = 'https://maxcvservices.dnb.com/V5.0/organizations/' + duns + '/products/DCP_STD'
        headers = {'Authorization': token}
        request = get(uri, headers=headers)
        response = request.json()
        if 'error' in response:
            log(message = 'error happened processing query', level = ERROR)
        else:
            row = OrderedDict()
            row['duns'] = duns
            row['details'] = dumps(response['OrderProductResponse']['OrderProductResponseDetail']['Product']['Organization'])
            yield row;


    def execute(self, quals, columns):
        if self.type == 'query':
            return self.query(quals, columns);
        elif self.type == 'details':
            return self.details(quals, columns);
