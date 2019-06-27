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
            'initials': get_elem_text(author_elem, 'Initials'),
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


def parse_entity(elem):
    article_path = 'MedlineCitation/Article/'

    parsed_dic = {
        'pmid': get_elem_text(elem, 'MedlineCitation/PMID'),
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