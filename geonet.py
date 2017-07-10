from . import AbstractPlugin
from ..elasticsearch_wrapper import elastic_conn
from datetime import datetime
from django.http import HttpResponse
from neogeo_xml_utils import ObjToXML
from pathlib import Path
import re
from urllib.parse import parse_qsl
from urllib.parse import urlparse


def group_by(seqs, i=0, merge=True):
    d = dict()
    for seq in seqs:
        k = seq[i]
        v = d.get(k, tuple()) + (seq[:i] + seq[i + 1:]
                                 if merge else (seq[:i] + seq[i + 1:]))
        d.update({k: v})
    return d


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

    def __init__(self, config, contexts):
        super().__init__(config, contexts)

        self.qs = [('any', 'Texte à rechercher', 'string'),
                   ('fast', "Activer le mode 'fast'", 'boolean'),
                   ('from', 'Index du premier document retourné', 'integer'),
                   ('to', 'Index du dernier document retourné', 'interger'),
                   ('type', 'Filtrer sur le type de resource', 'string')]

        self.opts = {'any': '',
                     'fast': False,
                     'from': self.FROM,
                     'to': self.TO,
                     'type': None}

        self._summary = {'categories': {'category': []},
                         'createDateYears': {'createDateYear': []},
                         # 'denominators': {'denominator': []},
                         'formats': {'format': []},
                         'inspireThemes': {'inspireTheme': []},
                         'inspireThemesWithAc': {'inspireThemeWithAc': []},
                         'keywords': {'keyword': []},
                         'licence': {'useLimitation': []},
                         # 'maintenanceAndUpdateFrequencies': {
                         #     'maintenanceAndUpdateFrequency': []},
                         'orgNames': {'orgName': []},
                         # 'resolutions': {'resolution': []},
                         # 'serviceTypes': {'serviceType': []},
                         # 'spatialRepresentationTypes': {
                         #     'spatialRepresentationType': []},
                         'status': {'status': []},
                         'types': {'type': []}}

    def input(self, **params):

        text_properties = ()
        for _, columns in self.columns_by_index.items():
            for typ, col in group_by(columns, i=1).items():
                if typ == 'text':
                    text_properties += col

        self.opts['any'] = ('any' in params) and params['any'] or None
        self.opts['fast'] = ('fast' in params and params['fast'] != 'false')
        self.opts['type'] = ('type' in params) and params['type'] or None

        try:
            self.opts['from'] = int(params['from'])
        except Exception:
            pass
        try:
            self.opts['to'] = int(params['to'])
        except Exception:
            pass
        if self.opts['from'] > self.opts['to']:
            self.opts['from'] = self.FROM
            self.opts['to'] = self.TO

        painless_script = (
            "if (params['_source']['origin']['source']['type'] == 'wfs') {"
            "return doc['origin.resource.metadata_url'].value}"
            "else if (params['_source']['origin']['source']['type'] == 'geonet') {"
            "return doc['origin.uuid'].value}")

        query = {
            'size': 0,
            'query': {
                'bool': {}},
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

        if self.opts['any']:
            query['query']['bool'].update({
                'must': {
                    'multi_match': {
                        'query': self.opts['any'],
                        'operator': 'or',
                        'fuzziness': 0.7,
                        'fields': ['properties.{0}'.format(p)
                                   for p in text_properties]}}})
        else:
            query['query']['bool'].update({
                'should': {
                    'match_all': {}}})

        if self.opts['type']:
            query['query']['bool'].update({
                'filter': {
                    'term': {
                        'origin.resource.name': self.opts['type']}}})

        return query

    def output(self, data, **params):
        count = 0
        metadata = []

        def update_metadata(hit):
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
                    res = {'@name': name, '@count': '0'}
                    for k, v in attrs.items():
                        res['@{0}'.format(k)] = v
                    self._summary[parent][e].append(res)

                for d in self._summary[parent][e]:
                    if d['@name'] == name:
                        d['@count'] = str(int(d['@count']) + 1)
                        break

            # categories/category
            category = data['info']['category']
            if isinstance(category, str):
                update_summary(
                    'categories', 'category', category,
                    label=dict(self.CATEGORIES).get(category, category))

            if isinstance(category, list):
                for val in category:
                    if isinstance(val, str):
                        update_summary(
                            'categories', 'category', val,
                            label=dict(self.CATEGORIES).get(val, val))
                    if isinstance(val, dict):
                        if '$' in val and val['$']:
                            update_summary(
                                'categories', 'category', val['$'],
                                label=dict(self.CATEGORIES).get(val['$'], val['$']))

            # createDateYears/createDateYear
            create_date = data['info']['createDate']
            date = datetime.strptime(create_date, '%Y-%m-%dT%H:%M:%S')
            update_summary('createDateYears', 'createDateYear', str(date.year))

            # denominators/denominator
            # TODO: impossible ???

            # formats/format
            if 'format' in data and data['format']:
                data_format = data['format']
                if isinstance(data_format, str):
                    update_summary('formats', 'format', data_format)
                if isinstance(data_format, list):
                    for val in data_format:
                        update_summary('formats', 'format', val)

            def update_keyword(val):
                update_summary('keywords', 'keyword', val)
                if val in (m[1] for m in self.INSPIRE_THEME):
                    # inspireThemes/inspireTheme
                    update_summary('inspireThemes', 'inspireTheme', val)

                    # inspireThemesWithAc/inspireThemeWithAc
                    ac = dict((m[1], m[0]) for m in self.INSPIRE_THEME).get(val, val)
                    update_summary('inspireThemesWithAc',
                                   'inspireThemeWithAc',
                                   '{0}|{1}'.format(ac, val))

            # keywords/keyword
            if 'keyword' in data and data['keyword']:
                keyword = data['keyword']
                if isinstance(keyword, str):
                    update_keyword(keyword)
                if isinstance(keyword, list):
                    for k in keyword:
                        update_keyword(k)

            # licence/useLimitation
            if 'LegalConstraints' in data and data['LegalConstraints']:
                for sub in data['LegalConstraints']:
                    if isinstance(sub, dict):
                        if sub['@preformatted'] == 'true':
                            continue
                        for k in ('useLimitation', 'otherConstraints'):
                            if k not in sub:
                                continue
                            val = sub[k]['CharacterString']
                            if not re.match('^(\w+\s*)+$', val):
                                continue
                            update_summary('licence', 'useLimitation', val)

            # rights == licence/useLimitation
            if 'rights' in data and data['rights']:
                rights = data['rights']
                if isinstance(rights, str):
                    update_summary('licence', 'useLimitation', rights)
                if isinstance(rights, list):
                    for val in rights:
                        update_summary('licence', 'useLimitation', val)

            # maintenanceAndUpdateFrequencies/maintenanceAndUpdateFrequency
            # TODO: impossible ???

            # orgNames/orgName
            if 'responsibleParty' in data and data['responsibleParty']:
                for val in data['responsibleParty']:
                    if isinstance(val, dict) and 'organisationName' in val:
                        update_summary('orgNames', 'orgName',
                                       val['organisationName'])

            # publisher == orgNames/orgName
            if 'publisher' in data and data['publisher']:
                publisher = data['publisher']
                if isinstance(publisher, str):
                    update_summary('orgNames', 'orgName', publisher)
                if isinstance(publisher, list):
                    for val in publisher:
                        update_summary('orgNames', 'orgName', val)

            # resolutions/resolution
            # TODO: impossible ???

            # serviceTypes/serviceType
            # TODO: impossible ???

            # spatialRepresentationTypes/spatialRepresentationType
            # TODO: impossible ???

            # status/status
            # TODO: impossible ???

            # types/type
            resource_type = hit['_source']['origin']['resource']['name']
            update_summary(
                'types', 'type', resource_type,
                label=dict(self.TYPE).get(resource_type, resource_type))

        # End update_metadata()

        # if not self.opts['any'] and not self.opts['type']:
        #     body = {'from': self.opts['from'],
        #             'size': self.opts['to'] - self.opts['from'] + 1,
        #             'query': {
        #                 'bool': {
        #                     'filter': [{
        #                         'term': {
        #                             'origin.source.type': 'geonet'}}],
        #                     'must': [{
        #                         'match_all': {}}]}}}
        #
        #     res = elastic_conn.search(index=self.INDEX, body=body)
        #     for hit in res['hits']['hits']:
        #         update_metadata(hit)
        #         count += 1
        #
        # else:
        uuid_list = []
        for bucket in data['aggregations']['metadata']['buckets']:
            try:
                # Il serait peut-être plus élégant d'effectuer ce parsing
                # dans le script painless envoyée à Elasticsearch au
                # moment de la requête (Cf. ligne 105)
                uuid = dict(parse_qsl(urlparse(bucket['key']).query))['ID']
            except Exception:
                uuid = bucket['key']
            # Et de gérer les doublons au moment même de la requête...
            if uuid not in uuid_list:
                # Pas de doublon, l'ordre du score est conservé,
                # il n'est donc pas nécesssaire de le vérifier ici.
                uuid_list.append(uuid)

        for i, uuid in enumerate(uuid_list):
            if i < self.opts['from']:
                continue
            if i > self.opts['to']:
                break

            body = {'_source': ['raw_data', 'origin.resource.name'],
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
                # Par défaut, on retourne uniquement le premier élément

            hit = res['hits']['hits'][0]
            update_metadata(hit)
            count += 1
        # End else

        self._summary['@count'] = str(count)

        data = {'response': {'@from': str(self.opts['from']),
                             '@to': str(self.opts['to']),
                             'metadata': metadata,
                             'summary': self._summary}}

        return HttpResponse(ObjToXML(data).tostring(),
                            content_type='application/xml')


plugin = Plugin
