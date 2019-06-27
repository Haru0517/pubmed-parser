from script.utils import fast_iter, write_to_json, pretty_print
from lxml import etree
import gzip
from pprint import pprint



def fast_iter(context, func):
    for event, elem in context:
        func(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context


def get_elem_text(par_elem, elem_name: str):
    if par_elem.find(elem_name) is not None:
        elem_text = (par_elem.find(elem_name).text or '').strip() or ''
    else:
        elem_text = ''
    return elem_text


def get_elem_dic(elem, elem_path: str, attrib: str):
    _elem_list = elem.findall(elem_path)

    _ret_dic = {}
    for _elem in _elem_list:
        _key = _elem.attrib.get(attrib, '')
        _text = _elem.text.strip() or ''
        _ret_dic[_key] = _text

    return _ret_dic


def get_elem_list(elem, elem_path: str):
    _elem_list = elem.findall(elem_path)
    _ret_list = [_elem.text.strip() or '' for _elem in _elem_list]
    return _ret_list



def get_authors_info(elem):
    authors_path = 'MedlineCitation/Article/AuthorList/Author'
    author_elem_list = elem.findall(authors_path)

    ret_authors_info = []
    for author_elem in author_elem_list:
        dic = {
            'lastname': get_elem_text(author_elem, 'LastName'),
            'forename': get_elem_text(author_elem, 'ForeName'),
            'initials': get_elem_text(author_elem, 'Initials')
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


def parse_entity(elem):
    article_path = 'MedlineCitation/Article/'

    parsed_dic = {
        # これでOK
        'pmid': get_elem_text(elem, 'MedlineCitation/PMID'),
        'title': get_elem_text(elem, article_path+'ArticleTitle'),
        'authors': get_authors_info(elem),  # {LastName, ForeName, Initials, affiliation}
        'pubdate': get_elem_text(elem, article_path+'Journal/JournalIssue/PubDate/Year'),
        'journal': get_elem_text(elem, article_path+'Journal/Title'),
        'abstract': get_elem_text(elem, article_path+'Abstract/AbstractText'),
        'publication_types': get_elem_dic(elem, 'MedlineCitation/Article/PublicationTypeList/PublicationType', 'UI'),  # 例：{D016428:Journal Art, D013487:Research Sup}
        'lang': get_elem_text(elem, article_path+'Language'),  # str
        'country': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/Country'),  # str
        'medline_ta': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/MedlineTA'),  # str
        'nlm_unique_id': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/NlmUniqueID'),  # str
        'issn_linking': get_elem_text(elem, 'MedlineCitation/MedlineJournalInfo/ISSNLinking'),  # str
        'other_id': get_elem_dic(elem, 'MedlineCitation/OtherID', 'Source'),  # {'nasa', 'pop'} otheridをfindallする
        'keywords': get_elem_list(elem, 'MedlineCitation/KeywordList/Keyword'),  # list YNはいらん?
        'mesh_terms': get_elem_dic(elem, 'MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName', 'UI'),  # {D000818:Animals; D001665:Binding Sites} qualifierもまとめて
        'chemical_list': get_elem_dic(elem, 'MedlineCitation/ChemicalList/Chemical/NameOfSubstance', 'UI'),  # {D002104:Cadmium; D002256:Carbonic}
        'grants': get_grant_info(elem),  # [{id, agency, country}, {}]
        'article_ids': get_elem_dic(elem, 'PubmedData/ArticleIdList/ArticleId', 'IdType'),  # articleIdList {'pubmed', 'pmc'}
        'references': get_reference_info(elem), # ReferenceList [{'citation': str, 'ids': {'pubmed', 'pmc'}}, {}]
        'delete': False,  # bool
    }
    """
    other abstract
    全部抽出しよう！
    """
    #pprint(parsed_dic)
    write_to_json('test.json', parsed_dic)


def parse_all():
    # globで全xml.gzをiterで読み込み forループ
    path = 'dataset/sample/sample.xml'

    # path設定
    if '.gz' in path:
        path = gzip.GzipFile(path)

    # 一つのファイルをiterparse
    tree = etree.iterparse(path, events=('start',), tag='PubmedArticle')

    # elemに分解して，parse_entityに渡す
    fast_iter(tree, parse_entity)


if __name__ == '__main__':
    parse_all()


"""
python -m script.test
で実行可能
"""