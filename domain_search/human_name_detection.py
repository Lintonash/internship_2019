import ner
import nltk
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from collections import defaultdict
import csv


def nltk_preprocess(text):
    text = word_tokenize(text)
    text = pos_tag(text)
    return text


def nltk_if_human_name(text):
    labels = defaultdict(int)
    ne_tree = nltk.ne_chunk(nltk_preprocess(text))
    for subtree in ne_tree:
        if hasattr(subtree, 'label'):
            labels[subtree.label()] += len(subtree)
        else:
            labels['no_label'] += 1
    if set(labels.keys()) == {'PERSON'}:
        # print(ne_tree, labels)
        return True

    return False


def main():
    counter = 0
    with open('output/shuffled1000-results-20190723-172028.csv', 'r', encoding='utf-8-sig') as f:
        df = [row for row in csv.DictReader(f)]
    for row in df:
        name = row['company_name']
        domain = row['domain']
        if domain and nltk_if_human_name(name):
            counter += 1
            print(domain)
    print(counter)

main()
# if_human_name('BEN GORDON CONSULTING')