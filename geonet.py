from ..elasticsearch_wrapper import elastic_conn
from . import AbstractPlugin
from datetime import datetime
from django.http import HttpResponse
from neogeo_xml_utils import ObjToXML
from pathlib import Path
from urllib.parse import urlparse, parse_qsl


class Plugin(AbstractPlugin):

    INDEX = Path(__file__).stem
    FROM = 0
    TO = 9
    TYPE = (('dataset', 'Série de données'),
            ('nonGeographicDataset', 'Jeux de données non géographiques'),
            ('series', 'Ensemble de séries de données'),
            ('service', 'Service'))
    INSPIRE_THEME = (
        ('ac', 'Conditions atmosphériques'),
        ('ad', 'Zones de gestion, de restriction ou de '
               'réglementation et unités de déclaration'),
        ('am', 'Adresses'),
        ('au', 'Unités administratives'),
        ('bu', 'Bâtiments'),
        ('cp', 'Parcelles cadastrales'),
        ('ef', 'Installations de suivi environnemental'),
        ('el', 'Altitude'),
        ('gg', 'Systèmes de maillage géographique'),
        ('hb', 'Habitats et biotopes'),
        ('hh', 'Santé et sécurité des personnes'),
        ('hy', 'Hydrographie'),
        ('lc', 'Occupation des terres'),
        ('lu', 'Usage des sols'),
        ('mf', 'Caractéristiques géographiques météorologiques'),
        ('oi', 'Ortho-imagerie'),
        ('ps', 'Sites protégés'),
        ('so', 'Sols'),
        ('tn', 'Réseaux de transport'),
        ('us', "Services d'utilité publique et services publics"))
    CATEGORIES = (('accessibilite', 'Accessibilité'),
                  ('citoyennete', 'Citoyenneté'),
                  ('culture', 'Culture'),
                  ('environnement', 'Environnement'),
                  ('equipements', 'Équipements'),
                  ('imagerie', 'Imagerie'),
                  ('limitesadministratives', 'Limites administratives'),
                  ('localisation', 'Localisation'),
                  ('occupationdusol', 'Occupation du sol'),
                  ('services', 'Services'),
                  ('transport', 'Transport'),
                  ('urbanisme', 'Urbanisme'))

    def __init__(self, config):
        super().__init__(config)

        self.qs = [('any', 'Texte à rechercher', 'string'),
                   ('fast', "Activer le mode 'fast'", 'boolean'),
                   ('from', 'Index du premier document retourné', 'integer'),
                   ('to', 'Index du dernier document retourné', 'integer'),
                   ('type', 'Filtrer sur le type de resource', 'string')]

        self.opts = {'any': '',
                     'fast': False,
                     'from': self.FROM,
                     'to': self.TO,
                     'type': None}

        self._summary = {'categories': {'category': []},
                         'createDateYears': {'createDateYear': []},
                         'denominators': {'denominator': []},
                         'formats': {'format': []},
                         'inspireThemes': {'inspireTheme': []},
                         'inspireThemesWithAc': {'inspireThemeWithAc': []},
                         'keywords': {'keyword': []},
                         'licence': {'useLimitation': []},
                         'maintenanceAndUpdateFrequencies': {
                                'maintenanceAndUpdateFrequency': []},
                         'orgNames': {'orgName': []},
                         'resolutions': {'resolution': []},
                         'serviceTypes': {'serviceType': []},
                         'spatialRepresentationTypes': {
                             'spatialRepresentationType': []},
                         'status': {'status': []},
                         'types': {'type': []}}

    def input(self, **params):

        self.opts.update(params)
        self.opts['fast'] = self.opts['fast'] == 'true' and True

        # C'est pas très beau...
        self.opts['from'] = int(self.opts['from'])
        self.opts['to'] = int(self.opts['to'])
        if self.opts['from'] > self.opts['to']:
            self.opts['from'] = self.FROM
            self.opts['to'] = self.TO

        # TODO: Fix origin.source.mode / origin.source.type

        painless_script = (
            "if (params['_source']['origin']['source']['mode'] == 'wfs') {"
            "return doc['origin.resource.metadata_url'].value}"
            "else if (params['_source']['origin']['source']['type'] == 'geonet') {"
            "return doc['origin.uuid'].value}")

        query = {
            'size': 0,
            'query': {
                'bool': {
                    'should': [{
                        'multi_match': {
                            'query': self.opts['any'],
                            'operator': 'or',
                            'fuzziness': 0.7,
                            'fields': ['properties.*']}}]}},
            'aggs': {
                'metadata': {
                    'aggs': {
                        'avg_score': {
                            'avg': {
                                'script': '_score'}}},
                    'terms': {
                        'order': {
                            'avg_score': 'desc'},
                        'script': {
                            'lang': 'painless',
                            'inline': painless_script},
                        'size': 9999999}}}}

        return query

    def output(self, data, **params):
        count = 0
        metadata = []

        def update_metadata(hit):

            type = hit['_source']['origin']['resource']['name']
            data = hit['_source']['raw_data']

            if self.opts['fast']:
                keys = ('category', 'changeDate',
                        'createDate', 'id', 'schema',
                        'selected', 'source', 'uuid')
                metadata.append(
                    {'info': dict((k, data['info'][k]) for k in keys)})
            else:
                metadata.append(data)

            # Puis m-à-j des éléments de <summary> lorsque cela est possible.

            def update_summary(parent, e, name, **attrs):
                if name not in (k['@name'] for k in self._summary[parent][e]):
                    res = {'@name': name, '@count': '1'}
                    for k, v in attrs.items():
                        res['@{0}'.format(k)] = v
                    self._summary[parent][e].append(res)

                for d in self._summary[parent][e]:
                    if d['@name'] == name:
                        d['@count'] = int(d['@count'])
                        d['@count'] += 1
                        d['@count'] = str(d['@count'])
                        break

            # categories/category
            for val in data['info']['category']:
                if isinstance(val, str):
                    update_summary('categories', 'category', val,
                                   label=dict(self.CATEGORIES)[val])
                if isinstance(val, dict):
                    if '$' in val and val['$']:
                        update_summary('categories', 'category', val['$'],
                                       label=dict(self.CATEGORIES)[val['$']])

            # createDateYears/createDateYear
            val = data['info']['createDate']
            date = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
            update_summary('createDateYears', 'createDateYear', str(date.year))

            # denominators/denominator
            # TODO

            # formats/format
            # TODO

            def update_keyword(val):
                update_summary('keywords', 'keyword', val)
                if val in (m[1] for m in self.INSPIRE_THEME):
                    # inspireThemes/inspireTheme
                    update_summary('inspireThemes', 'inspireTheme', val)

                    # inspireThemesWithAc/inspireThemeWithAc
                    ac = dict((m[1], m[0]) for m in self.INSPIRE_THEME)[val]
                    update_summary('inspireThemesWithAc',
                                   'inspireThemeWithAc',
                                   '{0}|{1}'.format(ac, val))

            # keywords/keyword
            if isinstance(data['keyword'], str):
                update_keyword(data['keyword'])
            if isinstance(data['keyword'], list):
                for keyword in data['keyword']:
                    update_keyword(keyword)

            # licence/useLimitation
            for sub in data['LegalConstraints']:
                if isinstance(sub, dict):
                    if sub['@preformatted'] == 'true':
                        continue
                    for k in ('useLimitation', 'otherConstraints'):
                        if k not in sub:
                            continue
                        update_summary('licence', 'useLimitation',
                                       sub[k]['CharacterString'])

            # maintenanceAndUpdateFrequencies/maintenanceAndUpdateFrequency
            # TODO

            # orgNames/orgName
            for val in data['responsibleParty']:
                if isinstance(val, dict) and 'organisationName' in val:
                    update_summary('orgNames', 'orgName',
                                   val['organisationName'])

            # resolutions/resolution
            # TODO

            # serviceTypes/serviceType
            # TODO

            # spatialRepresentationTypes/spatialRepresentationType
            # TODO

            # status/status
            # TODO

            # types/type
            update_summary('types', 'type', type, label=dict(self.TYPE)[type])

        # End update_metadata()

        if not self.opts['any']:
            body = {'from': self.opts['from'],
                    'size': self.opts['to'] - self.opts['from'] + 1,
                    'query': {
                        'bool': {
                            'filter': [{
                                'term': {
                                    'origin.source.type': 'geonet'}}],
                            'must': [{
                                'match_all': {}}]}}}

            res = elastic_conn.search(index=self.INDEX, body=body)
            for hit in res['hits']['hits']:
                update_metadata(hit)
                count += 1

        else:
            buckets = data['aggregations']['metadata']['buckets']
            for i, bucket in enumerate(buckets):
                if i < int(self.opts['from']):
                    continue
                if i > int(self.opts['to']):
                    break

                try:
                    # Il serait peut-être plus élégant d'effectuer ce parsing
                    # dans le script painless envoyée à Elasticsearch au
                    # moment de la requête (Cf. ligne 101)
                    uuid = dict(parse_qsl(urlparse(bucket['key']).query))['ID']
                except:
                    uuid = bucket['key']

                body = {'_source': [
                            'raw_data',
                            'origin.resource.name'],
                        'query': {
                            'match': {
                                'origin.uuid': uuid}}}

                res = elastic_conn.search(index=self.INDEX, body=body)

                if len(res['hits']['hits']) == 0:
                    continue
                if len(res['hits']['hits']) > 1:
                    import warnings
                    warnings.warn('Duplicate UUID.')
                    # Ce cas ne devrait JAMAIS arriver...
                    # Par défaut, l'on retourne le premier élément...

                hit = res['hits']['hits'][0]
                update_metadata(hit)
                count += 1

        self._summary['@count'] = str(count)

        data = {'response': {
                    '@from': str(self.opts['from']),
                    '@to': str(self.opts['to']),
                    'metadata': metadata,
                    'summary': self._summary}}

        return HttpResponse(
                    ObjToXML(data).tostring(), content_type='application/xml')


plugin = Plugin
