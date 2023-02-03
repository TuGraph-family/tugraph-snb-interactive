#!/bin/bash

cd load-scripts

echo "starting conversion"

cd import_data
rm -f *.csv

cd ..
python convert.py -i ../deps/ldbc_snb_datagen_hadoop/social_network -o import_data

cd import_data

# comment
ln -sf comment.csv comment_hasCreator_person.csv
ln -sf comment.csv comment_isLocatedIn_place.csv
# forum
ln -sf post.csv forum_containerOf_post.csv
ln -sf forum.csv forum_hasModerator_person.csv
# organisation
ln -sf organisation.csv organisation_isLocatedIn_place.csv
# person
ln -sf person.csv person_isLocatedIn_place.csv
# post
ln -sf post.csv post_hasCreator_person.csv
ln -sf post.csv post_isLocatedIn_place.csv
# tag
ln -sf tag.csv tag_hasType_tagclass.csv

echo "conversion finished"
