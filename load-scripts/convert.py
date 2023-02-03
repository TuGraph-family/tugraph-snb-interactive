import argparse
import os
import glob
import re
import multiprocessing

class Converter(object):

    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def __call__(self, prefix):
        input_filenames = glob.glob('%s/%s_[0-9]*_[0-9]*.csv' % (input_dir, prefix))
        output_filename = '%s/%s.csv' % (output_dir, os.path.basename(prefix))
        f = open(output_filename, 'w')
        if prefix == 'dynamic/comment':
            f1 = open('%s/comment_replyOf_post.csv' % output_dir, 'w')
            f2 = open('%s/comment_replyOf_comment.csv' % output_dir, 'w')
            for input_filename in input_filenames:
                header = False
                for line in open(input_filename):
                    if not header:
                        header = True
                        continue
                    columns = line.strip().split('|')
                    for i in range(len(columns)):
                        column = columns[i]
                        if column.find(',') >= 0 or column.startswith(' ') or column.endswith(' '):
                            columns[i] = '"%s"' % column
                    if columns[-2] != '':
                        f1.write('%s,%s,%s\n' % (columns[0], columns[-2], columns[1]))
                    else:
                        f2.write('%s,%s,%s\n' % (columns[0], columns[-1], columns[1]))
                    f.write(','.join(columns) + '\n')
            f1.close()
            f2.close()
        elif (prefix == 'static/place') or (prefix == 'static/tagclass'):
            if prefix == 'static/place':
                f1 = open('%s/place_isPartOf_place.csv' % output_dir, 'w')
            else:
                f1 = open('%s/tagclass_isSubclassOf_tagclass.csv' % output_dir, 'w')
            for input_filename in input_filenames:
                header = False
                for line in open(input_filename):
                    if not header:
                        header = True
                        continue
                    columns = line.strip().split('|')
                    for i in range(len(columns)):
                        column = columns[i]
                        if column.find(',') >= 0 or column.startswith(' ') or column.endswith(' '):
                            columns[i] = '"%s"' % column
                    if columns[-1] != '':
                        f1.write('%s,%s\n' % (columns[0], columns[-1]))
                    f.write(','.join(columns) + '\n')
            f1.close()
        elif (prefix == 'dynamic/person_knows_person'):
            for input_filename in input_filenames:
                header = False
                for line in open(input_filename):
                    if not header:
                        header = True
                        continue
                    columns = line.strip().split('|')
                    for i in range(len(columns)):
                        column = columns[i]
                        if column.find(',') >= 0 or column.startswith(' ') or column.endswith(' '):
                            columns[i] = '"%s"' % column
                    f.write(','.join(columns) + ',0\n')
        elif (prefix == 'dynamic/forum_hasMember_person'):
            for input_filename in input_filenames:
                header = False
                for line in open(input_filename):
                    if not header:
                        header = True
                        continue
                    columns = line.strip().split('|')
                    for i in range(len(columns)):
                        column = columns[i]
                        if column.find(',') >= 0 or column.startswith(' ') or column.endswith(' '):
                            columns[i] = '"%s"' % column
                    f.write(','.join(columns) + ',0,' + columns[0]  + '\n')
        else:
            for input_filename in input_filenames:
                header = False
                for line in open(input_filename):
                    if not header:
                        header = True
                        continue
                    columns = line.strip().split('|')
                    for i in range(len(columns)):
                        column = columns[i]
                        if column.find(',') >= 0 or column.startswith(' ') or column.endswith(' '):
                            columns[i] = '"%s"' % column
                    f.write(','.join(columns) + '\n')
        f.close()

ap = argparse.ArgumentParser(description='Convert SNB_Generator data (composite & foreign key) to LGraph CSV Files')
ap.add_argument('-i', dest='input_dir', action='store', help='input directory')
ap.add_argument('-o', dest='output_dir', action='store', help='output directory')
args = ap.parse_args()
input_dir = args.input_dir
output_dir = args.output_dir

if not os.path.isdir(output_dir):
    os.mkdir(output_dir)
pool = multiprocessing.Pool()
prefixes = [
    'static/organisation', 'static/place', 'static/tag', 'static/tagclass',
    'dynamic/comment', 'dynamic/comment_hasTag_tag',
    'dynamic/forum', 'dynamic/forum_hasMember_person', 'dynamic/forum_hasTag_tag',
    'dynamic/person', 'dynamic/person_hasInterest_tag', 'dynamic/person_knows_person',
    'dynamic/person_likes_comment', 'dynamic/person_likes_post',
    'dynamic/person_studyAt_organisation', 'dynamic/person_workAt_organisation',
    'dynamic/post', 'dynamic/post_hasTag_tag'
]
pool.map(Converter(input_dir, output_dir), prefixes)
