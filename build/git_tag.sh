#!/bin/sh

MD=`date '+%m%d'`
new_tag="tag_${MD}"

echo $new_tag

git fetch --tags
all_tags=`git tag -l`

echo $all_tags | grep -qi "${new_tag}"
[ $? = 0 ] &&
{
    echo "Tag [$new_tag] exists, testing for later tag"
    
    for x in {a..z}
    do
        new_tag="tag_${MD}_${x}"
        echo "Testing tag $new_tag"
        echo $all_tags | grep -qi "${new_tag}"
        [ $? = 0 ] ||
        { 
            break
        }
    done
}
echo "Using tag -- $new_tag"
git tag $new_tag 
git push --tags
