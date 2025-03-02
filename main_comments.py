from syncro_utils import  syncro_get_all_comments_from_csv, syncro_prepare_comments_json
from syncro_write import syncro_create_comment
from syncro_read import get_api_call_count
from syncro_configs import get_logger




def run_comments(config):
    logger = get_logger("main")

    try:
        comments = syncro_get_all_comments_from_csv(logger)  
        
        logger.info(f"Loaded comments: {len(comments)}")
    except Exception as e:
        logger.critical(f"Failed to load tickets: {e}")

    for comment in comments:
        comment_json = syncro_prepare_comments_json(comment)        
        logger.info(f"Attempting to create Comment: {comment_json}")
        syncro_create_comment(comment_json,config)
    
    
    api_call_count = get_api_call_count()
    logger.info(f"Total API calls made during program run: {api_call_count}")
        

if __name__ == "__main__":
    print("This is main_comments.py")
 