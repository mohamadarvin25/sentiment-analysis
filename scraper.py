"""
Google Play Store Review Scraper (Parallel Version)
Scrapes reviews from multiple popular Indonesian apps to get 10,000+ samples
Features: Parallel processing, detailed logging, progress tracking, retry mechanism
"""

import pandas as pd
from google_play_scraper import Sort, reviews_all
import time
from datetime import datetime
import json
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PlayStoreScraper:
    def __init__(self, max_workers=8):
        """
        Initialize scraper with parallel processing capability
        
        Args:
            max_workers: Number of parallel threads (default 8, good for DGX)
        """
        # List of popular Indonesian apps to scrape (Grab removed - no reviews)
        self.app_ids = [
            ('com.gojek.app', 'Gojek'),
            ('com.tokopedia.tkpd', 'Tokopedia'),
            ('com.shopee.id', 'Shopee'),
            ('com.instagram.android', 'Instagram'),
            ('com.whatsapp', 'WhatsApp'),
            ('com.spotify.music', 'Spotify'),
            ('com.netflix.mediaclient', 'Netflix'),
            ('id.dana', 'Dana'),
            ('com.traveloka.android', 'Traveloka'),
            ('com.bukalapak.android', 'Bukalapak'),
            ('com.lazada.android', 'Lazada'),
            ('id.co.bri.brimo', 'BRI Mobile'),
            ('com.dbs.id.digibank', 'digibank'),
            ('com.LinkAja', 'LinkAja'),
    ('com.ovo.id', 'OVO')   
        ]
        
        self.max_workers = max_workers
        self.stats = {
            'total_requested': 0,
            'total_collected': 0,
            'successful_apps': 0,
            'failed_apps': 0,
            'start_time': None,
            'end_time': None
        }
        
    def scrape_app_reviews(self, app_info, count=1000, max_retries=3):
        """
        Scrape reviews from a single app with retry mechanism
        
        Args:
            app_info: Tuple of (app_id, app_name)
            count: Number of reviews to scrape
            max_retries: Maximum retry attempts
        """
        app_id, app_name = app_info
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[{app_name}] Starting scrape (attempt {attempt}/{max_retries})...")
                
                start_time = time.time()
                
                result = reviews_all(
                    app_id,
                    sleep_milliseconds=0,
                    lang='id',  # Indonesian reviews
                    country='id',  # Indonesia
                    sort=Sort.NEWEST
                )
                
                # Limit to requested count
                result = result[:count]
                
                elapsed = time.time() - start_time
                
                logger.info(
                    f"[{app_name}] [OK] Successfully scraped {len(result)} reviews "
                    f"in {elapsed:.2f}s ({len(result)/elapsed:.1f} reviews/s)"
                )
                
                return {
                    'app_id': app_id,
                    'app_name': app_name,
                    'reviews': result,
                    'count': len(result),
                    'success': True,
                    'elapsed_time': elapsed
                }
                
            except Exception as e:
                logger.warning(
                    f"[{app_name}] [FAIL] Attempt {attempt}/{max_retries} failed: {str(e)}"
                )
                
                if attempt < max_retries:
                    wait_time = attempt * 2  # Exponential backoff
                    logger.info(f"[{app_name}] Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{app_name}] [FAIL] All attempts failed!")
                    return {
                        'app_id': app_id,
                        'app_name': app_name,
                        'reviews': [],
                        'count': 0,
                        'success': False,
                        'error': str(e)
                    }
        
        return None
    
    def scrape_all_parallel(self, target_count=3000, live_save=True):
        """
        Scrape reviews from all apps in parallel
        
        Args:
            target_count: Target number of total reviews
            live_save: If True, save reviews to CSV as they're collected
        """
        reviews_per_app = (target_count // len(self.app_ids)) + 200  # Buffer
        
        logger.info("="*80)
        logger.info("PARALLEL SCRAPING STARTED")
        logger.info("="*80)
        logger.info(f"Target total reviews: {target_count:,}")
        logger.info(f"Apps to scrape: {len(self.app_ids)}")
        logger.info(f"Reviews per app: ~{reviews_per_app:,}")
        logger.info(f"Max parallel workers: {self.max_workers}")
        logger.info(f"Live CSV updates: {'Enabled' if live_save else 'Disabled'}")
        logger.info(f"Estimated time: ~{len(self.app_ids)//self.max_workers * 2}-{len(self.app_ids)//self.max_workers * 3} minutes")
        logger.info("="*80)
        
        self.stats['start_time'] = datetime.now()
        self.stats['total_requested'] = reviews_per_app * len(self.app_ids)
        
        all_reviews = []
        results = []
        
        # Delete existing CSV if live_save enabled (start fresh)
        import os
        csv_filename = 'playstore_reviews.csv'
        if live_save and os.path.exists(csv_filename):
            os.remove(csv_filename)
            logger.info(f"Removed existing {csv_filename} for fresh start")
        
        # Parallel scraping with progress bar
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_app = {
                executor.submit(self.scrape_app_reviews, app_info, reviews_per_app): app_info 
                for app_info in self.app_ids
            }
            
            # Progress bar
            with tqdm(total=len(self.app_ids), desc="Scraping apps", unit="app") as pbar:
                for future in as_completed(future_to_app):
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        all_reviews.extend(result['reviews'])
                        self.stats['successful_apps'] += 1
                        
                        # Live save to CSV
                        if live_save and result['reviews']:
                            self.save_chunk_to_csv(result['reviews'], csv_filename, mode='a')
                            logger.info(f"[{result['app_name']}] Live saved {result['count']} reviews to CSV")
                    else:
                        self.stats['failed_apps'] += 1
                    
                    # Update progress bar with current total
                    pbar.set_postfix({
                        'collected': len(all_reviews),
                        'success': self.stats['successful_apps'],
                        'failed': self.stats['failed_apps']
                    })
                    pbar.update(1)
        
        self.stats['end_time'] = datetime.now()
        self.stats['total_collected'] = len(all_reviews)
        
        # Log summary
        self._print_summary(results)
        
        return all_reviews, results
    
    def _print_summary(self, results):
        """Print detailed summary of scraping results"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("SCRAPING SUMMARY")
        logger.info("="*80)
        logger.info(f"Total duration: {duration:.2f}s ({duration/60:.2f} minutes)")
        logger.info(f"Total reviews collected: {self.stats['total_collected']:,}")
        logger.info(f"Average speed: {self.stats['total_collected']/duration:.1f} reviews/second")
        logger.info(f"Successful apps: {self.stats['successful_apps']}/{len(self.app_ids)}")
        logger.info(f"Failed apps: {self.stats['failed_apps']}/{len(self.app_ids)}")
        logger.info("="*80)
        
        # Per-app breakdown
        logger.info("\nPER-APP BREAKDOWN:")
        logger.info("-"*80)
        
        # Sort by review count
        sorted_results = sorted(results, key=lambda x: x['count'], reverse=True)
        
        for result in sorted_results:
            status = "[OK]" if result['success'] else "[FAIL]"
            time_str = f"{result.get('elapsed_time', 0):.2f}s" if result['success'] else "N/A"
            logger.info(
                f"{status} {result['app_name']:<20} | "
                f"Reviews: {result['count']:>6,} | "
                f"Time: {time_str:>8}"
            )
        
        logger.info("="*80)
    
    def save_chunk_to_csv(self, reviews, filename='playstore_reviews.csv', mode='a'):
        """
        Incrementally save reviews to CSV (live update)
        
        Args:
            reviews: List of review dicts
            filename: Output CSV filename
            mode: 'a' for append, 'w' for overwrite
        """
        import os
        
        if not reviews:
            return
            
        df = pd.DataFrame(reviews)
        
        # Select relevant columns
        columns_to_keep = [
            'reviewId', 'userName', 'content', 'score', 
            'at', 'replyContent', 'appVersion', 'thumbsUpCount'
        ]
        
        df = df[columns_to_keep]
        df.columns = [
            'review_id', 'username', 'review_text', 'rating',
            'date', 'reply', 'app_version', 'helpful_count'
        ]
        
        # Write header only if file doesn't exist
        write_header = not os.path.exists(filename) or mode == 'w'
        df.to_csv(filename, mode=mode, header=write_header, index=False, encoding='utf-8')
    
    def save_to_csv(self, reviews, filename='playstore_reviews.csv'):
        """
        Convert reviews to DataFrame and save as CSV (final save)
        """
        logger.info(f"\nConverting {len(reviews)} reviews to DataFrame...")
        df = pd.DataFrame(reviews)
        
        # Select relevant columns
        columns_to_keep = [
            'reviewId',
            'userName', 
            'content',
            'score',
            'at',
            'replyContent',
            'appVersion',
            'thumbsUpCount'
        ]
        
        df = df[columns_to_keep]
        df.columns = [
            'review_id',
            'username',
            'review_text',
            'rating',
            'date',
            'reply',
            'app_version',
            'helpful_count'
        ]
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"[OK] Saved {len(df)} reviews to {filename} ({df.memory_usage(deep=True).sum()/1024/1024:.2f} MB)")
        
        return df
    
    def save_to_json(self, reviews, filename='playstore_reviews.json'):
        """
        Save reviews as JSON
        """
        logger.info(f"Saving {len(reviews)} reviews to JSON...")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2, default=str)
        
        import os
        file_size = os.path.getsize(filename) / 1024 / 1024
        logger.info(f"[OK] Saved to {filename} ({file_size:.2f} MB)")


def main():
    """
    Main scraping function with parallel processing
    """
    logger.info("="*80)
    logger.info("GOOGLE PLAY STORE REVIEW SCRAPER (PARALLEL VERSION)")
    logger.info("Optimized for DGX Tesla V100")
    logger.info("="*80)
    logger.info("")
    
    # Initialize scraper with 8 workers (good for DGX)
    scraper = PlayStoreScraper(max_workers=8)
    
    # Scrape reviews in parallel
    all_reviews, results = scraper.scrape_all_parallel(target_count=3000)  # Extra buffer
    
    if not all_reviews:
        logger.error("No reviews collected! Exiting...")
        return
    
    # Save to both CSV and JSON
    df = scraper.save_to_csv(all_reviews, 'playstore_reviews.csv')
    scraper.save_to_json(all_reviews, 'playstore_reviews.json')
    
    # Print dataset statistics
    logger.info("\n" + "="*80)
    logger.info("DATASET STATISTICS")
    logger.info("="*80)
    logger.info(f"Total reviews: {len(df):,}")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Unique users: {df['username'].nunique():,}")
    logger.info(f"Average review length: {df['review_text'].str.len().mean():.1f} characters")
    logger.info("")
    logger.info("Rating distribution:")
    for rating, count in df['rating'].value_counts().sort_index().items():
        percentage = count / len(df) * 100
        bar = "█" * int(percentage / 2)
        logger.info(f"  {rating} stars: {count:>6,} ({percentage:>5.1f}%) {bar}")
    logger.info("="*80)
    logger.info("")
    logger.info("[OK] Scraping completed successfully!")
    logger.info("[OK] Log file saved to: scraper.log")
    logger.info("[OK] CSV file: playstore_reviews.csv (live updated)")
    logger.info("[OK] JSON file: playstore_reviews.json")
    logger.info("="*80)


if __name__ == "__main__":
    main()