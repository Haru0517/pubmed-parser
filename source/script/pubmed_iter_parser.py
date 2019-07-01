import gzip
from glob import glob

from lxml import etree
from pymongo import MongoClient
from tqdm import tqdm
from functools import partial
from script.utils import make_logger
import traceback


# MongoDBのコレクションを取得
client = MongoClient('mongodb://mongo:27017', username='root', password='example')
db = client.pubmed_database
collection = db.pubmed_article


def fast_iter(context, func):
    for _, elem in context:
        func(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context


def get_elem_text(par_elem, elem_path: str):
    if par_elem.find(elem_path) is not None:
        elem_text = ''.join(par_elem.find(elem_path).itertext()).strip()
    else:
        elem_text = ''
    return elem_text


def  get_elem_attrib(par_elem,  elem_path: str, attrib: str):
    if par_elem.find(elem_path) is not None:
        elem_attrib = par_elem.find(elem_path).attrib.get(attrib, '')
    else:
        elem_attrib = ''
    return elem_attrib


def get_elem_dic(elem, elem_path: str, attrib: str):
    _elem_list = elem.findall(elem_path)

    _ret_dic = {}
    for _elem in _elem_list:
        _key = _elem.attrib.get(attrib, '')
        _text = ''.join(_elem.itertext()).strip()
        _ret_dic[_key] = _text

    return _ret_dic


def get_elem_list(elem, elem_path: str):
    _elem_list = elem.findall(elem_path)
    _ret_list = [''.join(_elem.itertext()).strip() for _elem in _elem_list]
    return _ret_list


def get_authors_info(elem):
    authors_path = 'MedlineCitation/Article/AuthorList/Author'
    author_elem_list = elem.findall(authors_path)

    ret_authors_info = []
    for author_elem in author_elem_list:
        dic = {
            'lastname': get_elem_text(author_elem, 'LastName'),
            'forename': get_elem_text(author_elem, 'ForeName'),
            'initials': get_elem_text(author_elem, 'Initials'),
            'collective':  get_elem_text(author_elem, 'CollectiveName'),
            'affiliation': get_elem_text(author_elem, 'AffiliationInfo/Affiliation')
        }
        ret_authors_info.append(dic)
    return ret_authors_info


def get_grant_info(elem):
    grant_path = 'MedlineCitation/Article/GrantList/Grant'
    grant_elem_list = elem.findall(grant_path)

    ret_grant_info = []
    for grant_elem in grant_elem_list:
        dic = {
            'grant_id': get_elem_text(grant_elem, 'GrantID'),
            'agency': get_elem_text(grant_elem, 'Agency'),
            'country': get_elem_text(grant_elem, 'Country')
        }
        ret_grant_info.append(dic)
    return ret_grant_info


def get_reference_info(elem):
    reference_path = 'PubmedData/ReferenceList/Reference'
    refer_elem_list = elem.findall(reference_path)

    ret_refer_info = []
    for refer_elem in refer_elem_list:
        dic = {
            'citation': get_elem_text(refer_elem, 'Citation'),
            'article_ids': get_elem_dic(refer_elem, 'ArticleIdList/ArticleId', 'IdType')
        }
        ret_refer_info.append(dic)
    return ret_refer_info


def get_comments_corrections_info(elem):
    cc_path = 'MedlineCitation/CommentsCorrectionsList/CommentsCorrections'
    cc_elem_list = elem.findall(cc_path)

    ret_cc_info = []
    for cc_elem in cc_elem_list:
        dic = {
            'ref_source': get_elem_text(cc_elem, 'RefSource'),
            'pmid': get_elem_text(cc_elem, 'PMID'),
        }
        ret_cc_info.append(dic)
    return ret_cc_info


def parse_entity(elem, base_xml):
    article_path = 'MedlineCitation/Article/'

    pmid = get_elem_text(elem, 'MedlineCitation/PMID')
    version = int(get_elem_attrib(elem, 'MedlineCitation/PMID', 'Version'))

    if collection.find_one({'_id': pmid,  'version': {'$gte': version}}):
        # これより新しいVersionのArticleが既に存在するので，飛ばす
        return

    parsed_dic = {
        '_id': pmid,
        'version':  version,
        'base_xml': base_xml,
        'date_completed': get_elem_text(elem, 'MedlineCitation/DateCompleted/Year'),
        'date_revised': get_elem_text(elem, 'MedlineCitation/DateRevised/Year'),
        'title': get_elem_text(elem, article_path+'ArticleTitle'),
        'authors': get_authors_info(elem),
        'pubdate': get_elem_text(elem, article_path+'Journal/JournalIssue/PubDate/Year'),
        'journal': get_elem_text(elem, article_path+'Journal/Title'),
        'volume': get_elem_text(elem, article_path + 'Journal/JournalIssue/Volume'),
        'issue': get_elem_text(elem, article_path + 'Journal/JournalIssue/Issue'),
        'page': get_elem_text(elem, article_path + 'Pagination/MedlinePgn'),
        'issn': get_elem_text(elem, article_path + 'Journal/ISSN'),
        'issn_linking': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/ISSNLinking'),
        'iso_abbreviation': get_elem_text(elem, article_path + 'Journal/ISOAbbreviation'),
        'medline_ta': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/MedlineTA'),
        'abstract': get_elem_list(elem, article_path+'Abstract/AbstractText'),
        'other_abstract': get_elem_list(elem, 'MedlineCitation/OtherAbstract/AbstractText'),
        'lang': get_elem_text(elem, article_path+'Language'),
        'country': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/Country'),
        'nlm_unique_id': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/NlmUniqueID'),
        'other_id': get_elem_dic(elem, 'MedlineCitation/OtherID', 'Source'),
        'publication_types': get_elem_dic(elem, 'MedlineCitation/Article/'
                                                'PublicationTypeList/PublicationType', 'UI'),
        'keywords': get_elem_list(elem, 'MedlineCitation/KeywordList/Keyword'),
        'mesh_terms': get_elem_dic(elem, 'MedlineCitation/MeshHeadingList/'
                                         'MeshHeading/DescriptorName', 'UI'),
        'chemical_list': get_elem_dic(elem, 'MedlineCitation/ChemicalList/'
                                            'Chemical/NameOfSubstance', 'UI'),
        'grants': get_grant_info(elem),
        'article_ids': get_elem_dic(elem, 'PubmedData/ArticleIdList/ArticleId', 'IdType'),
        'references': get_reference_info(elem),
        'number_of_reference': get_elem_text(elem, 'MedlineCitation/NumberOfReferences'),
        'citation_subset': get_elem_text(elem, 'MedlineCitation/CitationSubset'),
        'comments_corrections': get_comments_corrections_info(elem),
        'publication_status': get_elem_text(elem, 'PubmedData/PublicationStatus'),
    }

    # MongoDBに格納，versionが上なら上書き（古いVersionのやつは上で既に飛ばしてる）
    collection.replace_one({'_id': pmid}, parsed_dic, upsert=True)


def parse_all():
    # ログを読み込み
    with open('log/parsed_files.log', mode='r') as f:
        parsed_file_list = f.readlines()
    parsed_file_list = [f.rstrip('\n') for f in parsed_file_list]

    # loggerを作成
    progress_logger = make_logger(log_name='parser-log', filename='log/parser.log', mode='a')
    parsed_file_logger = make_logger(log_name='parsed-file-log', filename='log/parsed_files.log',
                                     mode='a', formatter='%(message)s')

    xml_dirs = ['dataset/baseline/*.xml.gz', 'dataset/updates/*.xml.gz']
    descs = ['Baseline', 'Updates']
    for i, xml_dir in enumerate(xml_dirs):
        for xml_path in tqdm(glob(xml_dir), desc=descs[i]):
            if xml_path in parsed_file_list:
                # 既にパースしたファイルは飛ばす
                continue
            try:
                tree = etree.iterparse(gzip.GzipFile(xml_path),
                                       events=('end',), tag='PubmedArticle')
                fast_iter(tree, partial(parse_entity, base_xml=xml_path))
                parsed_file_logger.debug(xml_path)
                progress_logger.debug(f'Complete: {xml_path}')
            except EOFError:
                progress_logger.warning(f'Broken file: {xml_path}')


def parse_select():
    # ログを読み込み
    with open('log/parsed_files.log', mode='r') as f:
        parsed_file_list = f.readlines()
    parsed_file_list = [f.rstrip('\n') for f in parsed_file_list]

    # loggerを作成
    progress_logger = make_logger(log_name='parser-log', filename='log/parser.log', mode='a')
    parsed_file_logger = make_logger(log_name='parsed-file-log', filename='log/parsed_files.log',
                                     mode='a', formatter='%(message)s')

    xml_files = ['dataset/baseline/pubmed19n0490.xml.gz', 'dataset/baseline/pubmed19n0482.xml.gz',
                 'dataset/baseline/pubmed19n0370.xml.gz', 'dataset/updates/pubmed19n0974.xml.gz']
    for xml_path in tqdm(xml_files):
        if xml_path in parsed_file_list:
            # 既にパースしたファイルは飛ばす
            continue
        try:
            tree = etree.iterparse(gzip.GzipFile(xml_path),
                                   events=('end',), tag='PubmedArticle')
            fast_iter(tree, partial(parse_entity, base_xml=xml_path))
            parsed_file_logger.debug(xml_path)
            progress_logger.debug(f'Complete: {xml_path}')
        except EOFError:
            progress_logger.warning(f'Broken file: {xml_path}')



def test():
    node = etree.fromstring("""
    <content>
        Text outside tag <div>Text <em>inside</em> tag</div>
    </content>
    """)

    print(''.join(node.itertext()))
    logger = make_logger('test', 'test.log', 'w')
    logger.debug(''.join(node.itertext()).strip())
    logger.debug('aaaa')


if __name__ == '__main__':
    # parse_all()
    parse_select()
    # test()


"""
python -m script.pubmed_iter_parser
で実行可能
"""
