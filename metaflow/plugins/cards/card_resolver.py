from .card_datastore import CardDatastore,stepname_from_card_id,NUM_SHORT_HASH_CHARS
from .exception import  CardNotPresentException

def resolve_paths_from_task(flow_datastore,\
                            run_id,\
                            step_name,\
                            task_id,\
                            pathspec=None,\
                            type=None, \
                            card_id=None, \
                            index=None, \
                            hash=None):
    card_datastore = CardDatastore(flow_datastore,\
                                run_id,\
                                step_name,\
                                task_id,\
                                path_spec=pathspec)
    card_paths_found = card_datastore.extract_card_paths(
        card_type=type,card_id=card_id,card_index=index,card_hash=hash
    )
    return card_paths_found,card_datastore