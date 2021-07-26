from bson import json_util
from faker import Faker
import argparse
import bson
import os
import yaml
import pymongo

fake = Faker()
client = pymongo.MongoClient("localhost", 27017)

def flatten(d, sep='_', parent_key=''):
    """Flattens a nested map.

    Adapted from https://stackoverflow.com/a/6027615/254190"""
    items=dict()
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if v and isinstance(v, dict):
            items = {**items, **flatten(v, sep, new_key)}
        else:
            items[new_key] = v
    return items

def vacate_collection(db_name, collection_name):
    """Removes all records from a collection"""
    coll = client[db_name][collection_name]
    docs_count = coll.count_documents({})
    print("-- Removing", docs_count, "documents...")
    coll.delete_many({})
    return

def str_to_generator(fk, s):
    """Converts an object - usually a string - into a generator.

    'delete' is a special case where we simply return False. We expect the
    caller to understand what this means.

    We would like this to support lists and maps (i.e. recursion) at some point.
    Patches welcome!
    """
    if s == "delete":
        return False
    if isinstance(s, list):
        raise Exception("Lists -- especially lists of maps -- are not yet supported!")
    else:
        return getattr(fk, s)

def remove_missing_keys(a, b):
    """Remove keys from a that don't appear in b. Expects both maps to be flat"""
    n = dict()
    for k in b.keys():
        if k in a:
            n[k] = a[k]
    return n

def transform_values(db_name, collection_name, set_generator, unset_generator):
    """Iterate through a collection (identified by db_name.collection_name, e.g. foo.bar),
    updating each record by calling the provided 'set' and 'unset' generators.
    This means a new, random 'set' (and 'unset' - although this doesn't really make sense)
    for each record.
    """
    coll = client[db_name][collection_name]
    docs_count = coll.count_documents({})
    print("-- Updating", docs_count, "documents...")
    for result in coll.find({}):
        tries = 10
        while tries > 0:
            # TODO values that don't exist on the record
            # should be removed from the set.
            new_set = remove_missing_keys(set_generator(), flatten(result, '.'))
            try:
                coll.update_one({ '_id': result['_id']},
                                {'$set'   : new_set,
                                 '$unset' : unset_generator()})
                docs_count = docs_count - 1;
                break
            except pymongo.errors.DuplicateKeyError:
                if --tries > 0:
                    continue
                else:
                    print("Generator failed to produce a unique value after 10 tries. Aborting.")
                    exit(1)


def prepare_generators(m, d, p=[]):
    """Converts a nested map of string:string into a flat map of string:generator.

    Nested keys are converted to flat using `.` notation (e.g. foo.bar) and
    string values are converted to generators (see `str_to_generator`)
    """
    for field_name, transformer_or_field in m.items():
        if isinstance(transformer_or_field, dict):
            x = prepare_generators(transformer_or_field, dict(), p+[field_name])
            d = {**d, **x}
        else:
            generator = str_to_generator(fake, transformer_or_field)
            d[".".join(p+[field_name])] = generator
    return d

def create_set_generator(generator_cfg):
    """Converts a flat map of string:generators into a single generator function
    which is designed for 'set' operation in a MongoDB update.

    'False' generator values are discarded.
    """
    def gen():
        d = dict()
        for field_name, transformer in generator_cfg.items():
            if transformer != False:
                d[field_name] = transformer()
        return d
    return gen

def create_unset_generator(generator_cfg):
    """Converts a flat map of string:generators into a single generator function
    which is designed for 'unset' operation in a MongoDB update.

    'False' generators are used to indicate that values should be 'unset'."""
    def gen():
        d = dict()
        for field_name, transformer in generator_cfg.items():
            if transformer == False:
                d[field_name] = 1
        return d
    return gen

def apply_cfg(cfg):
    """Applies a fog config to a local MongoDB instance.

    The structure of the config is as follows:
    {'namespace':{'collection':{'field':'generator'}}}

    e.g.
    {'my_db':{'users':{'email':'ascii_safe_email'}}}

    In this case, all records in 'my_db.users' will have a random email,
    generated from `faker.ascii_safe_email` applied to thw `email` field.

    """
    # dbs
    for db_name, db in cfg.items():
        print("db:", db_name )
        # collections
        for collection_name, collection  in db.items():
            print("- collection:", collection_name)
            if collection == "delete":
                vacate_collection(db_name, collection_name)
            else:
                generator_cfg   = prepare_generators(collection, dict())
                set_generator   = create_set_generator(generator_cfg)
                unset_generator = create_unset_generator(generator_cfg)
                transform_values(db_name, collection_name, set_generator, unset_generator)
    return

def fog(fog_cfg_path):
    """Load a yaml file as a fog config and apply"""
    with open(fog_cfg_path, 'r') as stream:
        try:
            cfg = yaml.safe_load(stream)
            apply_cfg(cfg)
        except yaml.YAMLError as exc:
            print(exc)
    return

if __name__ == '__main__':
    # pylint: disable=invalid-name
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', help='list mongodb tables', action='store_true',)
    parser.add_argument('--fog',  help='config file')
    args = parser.parse_args()

    if args.fog:
        print("Using fog config:", args.fog.strip())
        fog(args.fog)
    elif args.list:
        print(client.list_database_names())
    else:
        print("No action specified")
