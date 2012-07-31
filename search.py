""" 
Bing Search API available through Windows Azure Market. The search API 
requires creating Primary Account Key (API key). 

If you have registered for the service, your account key should be visible
 at https://datamarket.azure.com/account/keys

Bing API allows search over 5 features

* web
* image
* video
* news
* spell

All 5 can be pulled together using Composite

Authentication for the API is via basic auth
"""
from requests import get 
from requests.auth import HTTPBasicAuth as HttpAuth
from json import loads
import re
class BingSearchRuntimeInvalidParameter(Exception):
    pass


class BingSearchInvalidParameter(Exception):
    pass


class BingSearchAuthenticationFailure(Exception):
    pass





class BingSearchFactory(object):
    valid_sources = set([ 'web', 
                      'image', 
                      'video',
                      'news', 
                      'spell',
                      'all'])
    valid_market = set([ "ar-XA",
                          "bg-BG",
                          "cs-CZ",
                          "da-DK",
                          "de-AT",
                          "de-CH",
                          "de-DE",
                          "el-GR",
                          "en-AU",
                          "en-CA",
                          "en-GB",
                          "en-ID",
                          "en-IE",
                          "en-IN",
                          "en-MY",
                          "en-NZ",
                          "en-PH",
                          "en-SG",
                          "en-US",
                          "en-XA",
                          "en-ZA",
                          "es-AR",
                          "es-CL",
                          "es-ES",
                          "es-MX",
                          "es-US",
                          "es-XL",
                          "et-EE",
                          "fi-FI",
                          "fr-BE",
                          "fr-CA",
                          "fr-CH",
                          "fr-FR",
                          "he-IL",
                          "hr-HR",
                          "hu-HU",
                          "it-IT",
                          "ja-JP",
                          "ko-KR",
                          "lt-LT",
                          "lv-LV",
                          "nb-NO",
                          "nl-BE",
                          "nl-NL",
                          "pl-PL",
                          "pt-BR",
                          "pt-PT",
                          "ro-RO",
                          "ru-RU",
                          "sk-SK",
                          "sl-SL",
                          "sv-SE",
                          "th-TH",
                          "tr-TR",
                          "uk-UA",
                          "zh-CN",
                          "zh-HK",
                          "zh-TW", 
                           None])
    valid_file_type = set(["DOC",
                           "DWF",
                           "FEED",
                           "HTM",
                           "HTML",
                           "PDF",
                           "PPT",
                           "RTF",
                           "TEXT",
                           "TXT",
                           "XLS",
                            None, ])
    first_cap_re = re.compile('(.)([A-Z][a-z]+)')
    all_cap_re = re.compile('([a-z0-9])([A-Z])')
    valid_adult = set(['Off', 'Moderate', 'Strict', None])
    SEARCH_ENDPOINT = 'https://api.datamarket.azure.com/Data.ashx/Bing/Search/Composite'
    def __init__(self, app_id):
        """
        BingSearchFactory creates instances of searches 
        """
        self.app_id = app_id
        
    
    def crazy_query(self,query_params):
        q = {}
        for key, value in query_params.items():
            if value == None:
                continue
            if not key.startswith("$"):
                value = "'%s'" % value
            q[key] = value
        return q

    

    def data_delivery(self, data_set):
        if data_set.has_key('ID'):
            del data_set['ID']
        if data_set.has_key('__metadata'):
            del data_set['__metadata']
        ds = {}
        for k, v in data_set.items():
            s1 = self.first_cap_re.sub(r'\1_\2', k)
            k = self.all_cap_re.sub(r'\1_\2', s1).lower()
            if isinstance(v, dict):
                v = self.data_delivery(v)
            ds[k] = v
        return ds

    def search(self,query, sources='web', market=None, adult=None, file_type=None, image_filters=None, video_filters=None, top=None, skip=None):
        if len(self.valid_sources.intersection([sources])) == 0:
            raise BingSearchInvalidParameter('Invalid sources argument')
        if len(self.valid_market.intersection([market])) == 0:
            raise BingSearchInvalidParameter('Invalid market argument')
        if len(self.valid_adult.intersection([adult])) == 0:
            raise BingSearchInvalidParameter('Invalid adult argument')
        if len(self.valid_file_type.intersection([file_type])) == 0:
            raise BingSearchInvalidParameter('Invalid file_type argument')
        _next = (50, 0)
        auth = HttpAuth(self.app_id, self.app_id)
        if sources == 'all':
            sources = 'web+image+video+news+spell'
        
        p_data = { 'Query':  query,
                   'Sources': sources,
                   'Market': market,
                   'WebFileType': file_type,
                   'ImageFilters': image_filters,
                   'VideoFilters': video_filters,
                   '$format':'Json',
                   '$top': top,
                   '$skip': skip
                   }
        p_data = self.crazy_query(p_data)
        
        qr =  get(self.SEARCH_ENDPOINT, params=p_data, auth=auth)
        if qr.status_code == 401:
            raise BingSearchAuthenticationFailure()
        if qr.status_code == 400:
            raise BingSearchRuntimeInvalidParameter()
        if qr.status_code == 200:
            qd = loads(qr.content) 
            data_point = sources.capitalize()
            data_set = qd['d']['results'][0][data_point]
            for ds in data_set:
                yield self.data_delivery(ds)
                
            
