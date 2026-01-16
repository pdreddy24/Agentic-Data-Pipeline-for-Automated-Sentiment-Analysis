import itertools
import json
import os
import re
import time
import logging
from datetime import timedelta, datetime
from dotenv import load_dotenv

import openai
from airflow.decorators import dag, task
from airflow.models.param import Param

# ---------------------- LOAD ENV ----------------------
load_dotenv()
openai_key = os.getenv("OPENAI_API_KEY")

logger = logging.getLogger(__name__)

# ---------------------- CONFIG ----------------------
class Config:
    BASE_DIR = os.getenv('PIPELINE_BASE_DIR', '/Users/airscholar/PycharmProjects/SelfHealingPipeline')
    INPUT_FILE = os.getenv('PIPELINE_INPUT_FILE', f'{BASE_DIR}/input/yelp_academic_dataset_review.json')
    OUTPUT_DIR = os.getenv('PIPELINE_OUTPUT_DIR', f'{BASE_DIR}/output/')
    MAX_TEXT_LENGTH = int(os.getenv('PIPELINE_MAX_TEXT_LENGTH', 2000))
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_OFFSET = 0

    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
    OPENAI_TIMEOUT = int(os.getenv('OPENAI_TIMEOUT', 120))
    OPENAI_RETRIES = int(os.getenv('OPENAI_RETRIES', 3))

# ---------------------- DEFAULT ARGS ----------------------
default_args = {
    'owner': 'Deekshitha Reddy',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
}

# ---------------------- HELPER FUNCTIONS ----------------------
def _load_openai_model(model_name: str):
    openai.api_key = Config.OPENAI_API_KEY
    logger.info(f"Using OpenAI model: {model_name}")
    try:
        response = openai.ChatCompletion.create(
            model=model_name,
            messages=[{
                "role": "user",
                "content": "Classify sentiment: 'This is a great product!' as POSITIVE, NEGATIVE, or NEUTRAL. Respond in JSON like {\"sentiment\": \"POSITIVE\", \"confidence\": 0.95}."
            }],
            timeout=Config.OPENAI_TIMEOUT
        )
        logger.info(f'Model validation response: {response.choices[0].message.content.strip()}')
    except Exception as e:
        logger.error(f"OpenAI validation failed: {e}")
        raise
    return {
        'backend': 'openai',
        'model_name': model_name,
        'status': 'loaded',
        'validated_at': datetime.now().isoformat(),
    }

def _load_from_file(batch_size: int = Config.DEFAULT_BATCH_SIZE, offset: int = Config.DEFAULT_OFFSET):
    if not os.path.exists(Config.INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {Config.INPUT_FILE}")
    reviews = []
    with open(Config.INPUT_FILE, 'r', encoding='utf-8') as f:
        sliced = itertools.islice(f, offset, offset + batch_size)
        for line in sliced:
            try:
                review = json.loads(line.strip())
                reviews.append({
                    'review_id': review.get('review_id'),
                    'business_id': review.get('business_id'),
                    'user_id': review.get('user_id'),
                    'stars': review.get('stars', 0),
                    'text': review.get('text'),
                    'date': review.get('date'),
                    'useful': review.get('useful', 0),
                    'funny': review.get('funny', 0),
                    'cool': review.get('cool', 0),
                })
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line: {e}")
                continue
    logger.info(f'Loaded {len(reviews)} reviews from file starting at offset {offset}.')
    return reviews

def _heal_review(review: dict) -> dict:
    text = review.get('text', '')
    result = {
        'review_id': review.get('review_id'),
        'business_id': review.get('business_id'),
        'stars': review.get('stars', 0),
        'original_text': None,
        'error_type': None,
        'action_taken': 'none',
        'was_healed': False,
        'metadata': {
            'user_id': review.get('user_id'),
            'date': review.get('date'),
            'useful': review.get('useful', 0),
            'funny': review.get('funny', 0),
            'cool': review.get('cool', 0),
        }
    }

    if isinstance(text, (str, int, float, bool, type(None))):
        result['original_text'] = text
    else:
        result['original_text'] = str(text) if text else None

    if text is None or not str(text).strip():
        result['error_type'] = 'missing_or_empty'
        result['healed_text'] = 'No review text provided.'
        result['action_taken'] = 'filled_with_placeholder'
        result['was_healed'] = True
    elif not re.search(r'[a-zA-Z0-9]', str(text)):
        result['error_type'] = 'special_characters_only'
        result['healed_text'] = '[Non-text content]'
        result['action_taken'] = 'replaced_special_characters'
        result['was_healed'] = True
    elif len(str(text)) > Config.MAX_TEXT_LENGTH:
        result['error_type'] = 'too_long'
        result['healed_text'] = str(text)[:Config.MAX_TEXT_LENGTH-3] + '...'
        result['action_taken'] = 'truncated_text'
        result['was_healed'] = True
    else:
        result['healed_text'] = str(text).strip()

    return result

def _parse_openai_response(response_text: str):
    try:
        clean_text = response_text.strip()
        if clean_text.startswith('```'):
            lines = clean_text.split('\n')
            clean_text = '\n'.join(lines[1:-1]) if lines[-1].strip() == '```' else '\n'.join(lines[1:])
        parsed = json.loads(clean_text)
        sentiment = parsed.get('sentiment', 'NEUTRAL').upper()
        confidence = float(parsed.get('confidence', 0.0))
        if sentiment not in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']:
            sentiment = 'NEUTRAL'
        return {'label': sentiment, 'score': min(max(confidence, 0.0), 1.0)}
    except Exception:
        upper_text = response_text.upper()
        if 'POSITIVE' in upper_text: return {'label': 'POSITIVE', 'score': 0.75}
        if 'NEGATIVE' in upper_text: return {'label': 'NEGATIVE', 'score': 0.75}
        return {'label': 'NEUTRAL', 'score': 0.5}

def _analyze_with_openai(healed_reviews: list, model_info: dict) -> list:
    model_name = model_info.get('model_name')
    results = []
    total = len(healed_reviews)
    openai.api_key = Config.OPENAI_API_KEY

    for idx, review in enumerate(healed_reviews):
        text = review.get('healed_text', '')
        prediction = None

        for attempt in range(Config.OPENAI_RETRIES):
            try:
                prompt = f"""
                Analyze the sentiment of this review and classify it as POSITIVE, NEGATIVE, or NEUTRAL.
                Review: "{text}"
                Respond ONLY in JSON: {{"sentiment": "POSITIVE", "confidence": 0.95}}
                """
                response = openai.ChatCompletion.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    timeout=Config.OPENAI_TIMEOUT
                )
                prediction = _parse_openai_response(response.choices[0].message.content.strip())
                break
            except Exception as e:
                if attempt < Config.OPENAI_RETRIES - 1:
                    logger.warning(f'Attempt {attempt+1} failed for review {review.get("review_id")}: {e}. Retrying...')
                    time.sleep(1)
                else:
                    logger.error(f'All attempts failed for review {review.get("review_id")}: {e}.')
                    prediction = {'label': 'NEUTRAL', 'score': 0.5, 'error': str(e)}

        if (idx + 1) % 10 == 0 or (idx + 1) == total:
            logger.info(f'Processed {idx + 1}/{total} reviews for sentiment analysis.')

        results.append({
            'review_id': review.get('review_id'),
            'business_id': review.get('business_id'),
            'stars': review.get('stars', 0),
            'text': review.get('healed_text', ''),
            'original_text': review.get('original_text', ''),
            'predicted_sentiment': prediction.get('label'),
            'confidence': round(prediction.get('score'), 4),
            'status': 'healed' if review.get('was_healed') else 'success',
            'healing_applied': review.get('was_healed'),
            'healing_action': review.get('action_taken') if review.get('was_healed') else None,
            'error_type': review.get('error_type') if review.get('was_healed') else None,
            'metadata': review.get('metadata', {}),
        })
    return results

# ---------------------- DAG ----------------------
@dag(
    dag_id='agentic_pipeline_dag',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['self-healing', 'sentiment-analysis']
)
def agentic_pipeline():
    
    @task
    def load_reviews(batch_size: int = Config.DEFAULT_BATCH_SIZE, offset: int = Config.DEFAULT_OFFSET):
        return _load_from_file(batch_size=batch_size, offset=offset)
    
    @task
    def heal_reviews(reviews: list):
        return [_heal_review(r) for r in reviews]
    
    @task
    def analyze_sentiment(reviews: list):
        model_info = _load_openai_model(Config.OPENAI_MODEL)
        return _analyze_with_openai(reviews, model_info)
    
    reviews = load_reviews()
    healed = heal_reviews(reviews)
    analyzed = analyze_sentiment(healed)
    
    return analyzed

dag = agentic_pipeline()
