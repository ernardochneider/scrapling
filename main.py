from scrapling import StealthyFetcher
import re
import os
import psycopg2
from datetime import datetime, date
import time
import sys


def get_db_connection():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("环境变量 DATABASE_URL 未设置")
    return psycopg2.connect(database_url)


def init_db(conn):
    """初始化数据库表"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jable_hot_rankings (
                id               SERIAL PRIMARY KEY,
                scraped_date     DATE NOT NULL,
                rank             INTEGER NOT NULL,
                video_id         VARCHAR(100) NOT NULL,
                video_id_num     VARCHAR(50),
                title            TEXT NOT NULL,
                url              TEXT NOT NULL,
                duration         VARCHAR(20),
                thumbnail        TEXT,
                preview          TEXT,
                hls_url          TEXT,
                views            INTEGER DEFAULT 0,
                likes            INTEGER DEFAULT 0,
                scraped_at       TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (scraped_date, video_id)
            );

            CREATE INDEX IF NOT EXISTS idx_rankings_date
                ON jable_hot_rankings (scraped_date);
            CREATE INDEX IF NOT EXISTS idx_rankings_video_id
                ON jable_hot_rankings (video_id);
            CREATE INDEX IF NOT EXISTS idx_rankings_rank
                ON jable_hot_rankings (scraped_date, rank);
        """)
        conn.commit()
    print("数据库表初始化完成")


def save_videos_to_db(conn, videos):
    """将每日热榜数据写入数据库（同一天同一视频只保留一条）"""
    upsert_sql = """
        INSERT INTO jable_hot_rankings (
            scraped_date, rank, video_id, video_id_num,
            title, url, duration, thumbnail, preview,
            hls_url, views, likes, scraped_at
        ) VALUES (
            %(scraped_date)s, %(rank)s, %(video_id)s, %(video_id_num)s,
            %(title)s, %(url)s, %(duration)s, %(thumbnail)s, %(preview)s,
            %(hls_url)s, %(views)s, %(likes)s, %(scraped_at)s
        )
        ON CONFLICT (scraped_date, video_id) DO UPDATE SET
            rank         = EXCLUDED.rank,
            views        = EXCLUDED.views,
            likes        = EXCLUDED.likes,
            hls_url      = EXCLUDED.hls_url,
            scraped_at   = EXCLUDED.scraped_at;
    """

    success_count = 0
    fail_count = 0

    with conn.cursor() as cur:
        for video in videos:
            if not video.get('video_id') or not video.get('title'):
                print(f"  跳过无效数据: {video.get('url', '未知URL')}")
                fail_count += 1
                continue

            try:
                cur.execute(upsert_sql, {
                    'scraped_date': video.get('scraped_date'),
                    'rank':         video.get('rank'),
                    'video_id':     video.get('video_id'),
                    'video_id_num': video.get('video_id_num'),
                    'title':        video.get('title'),
                    'url':          video.get('url'),
                    'duration':     video.get('duration'),
                    'thumbnail':    video.get('thumbnail'),
                    'preview':      video.get('preview'),
                    'hls_url':      video.get('hls_url'),
                    'views':        video.get('views', 0),
                    'likes':        video.get('likes', 0),
                    'scraped_at':   video.get('scraped_at'),
                })
                success_count += 1
            except Exception as e:
                print(f"  插入失败 [{video.get('video_id')}]: {str(e)}")
                conn.rollback()
                fail_count += 1
                continue

        conn.commit()

    print(f"数据库写入完成: 成功 {success_count} 条，失败 {fail_count} 条")
    return success_count, fail_count


def extract_hls_url(video_url):
    """从视频页面提取HLS链接"""
    try:
        page = StealthyFetcher.fetch(video_url)
        html_content = page.html_content

        hls_patterns = [
            r"var hlsUrl = '([^']+)'",
            r'var hlsUrl = "([^"]+)"',
            r'hlsUrl\s*=\s*["\']([^"\']+)["\']'
        ]

        for pattern in hls_patterns:
            match = re.search(pattern, html_content)
            if match and '.m3u8' in match.group(1):
                return match.group(1)

        return None

    except Exception as e:
        print(f"提取HLS链接失败 {video_url}: {str(e)}")
        return None


def scrape_jable_videos():
    """抓取Jable热门视频（全量）"""
    print("开始抓取 Jable.tv 热门视频...")

    page = StealthyFetcher.fetch('https://jable.tv/hot/')
    video_boxes = page.css('.video-img-box')

    total_videos = len(video_boxes)
    print(f"找到 {total_videos} 个视频，全部处理")

    videos = []

    for idx, detail in enumerate(video_boxes, 1):
        print(f"[{idx}/{total_videos}] 处理视频...")

        video_info = {}
        video_info['rank'] = idx

        title_elem = detail.css('.title a')
        if title_elem:
            video_info['title'] = title_elem[0].text.strip()
            video_info['url'] = title_elem[0].attrib.get('href', '')

        if video_info.get('url'):
            match = re.search(r'/videos/([^/]+)/', video_info['url'])
            if match:
                video_info['video_id'] = match.group(1)

        sub_title = detail.css('.sub-title')
        if sub_title:
            sub_html = sub_title[0].html_content
            numbers = re.findall(r'>\s*([\d\s]+)\s*<', sub_html)
            numbers = [n.strip() for n in numbers if n.strip() and re.search(r'\d', n)]

            if len(numbers) >= 2:
                try:
                    video_info['views'] = int(numbers[0].replace(' ', ''))
                    video_info['likes'] = int(numbers[1].replace(' ', ''))
                except:
                    video_info['views'] = 0
                    video_info['likes'] = 0

        duration_elem = detail.css('.label')
        if duration_elem:
            video_info['duration'] = duration_elem[0].text.strip()

        img_elem = detail.css('img.lazyload')
        if img_elem:
            video_info['thumbnail'] = img_elem[0].attrib.get('data-src', '')
            video_info['preview'] = img_elem[0].attrib.get('data-preview', '')

        fav_elem = detail.css('[data-fav-video-id]')
        if fav_elem:
            video_info['video_id_num'] = fav_elem[0].attrib.get('data-fav-video-id', '')

        if video_info.get('url'):
            hls_url = extract_hls_url(video_info['url'])
            if hls_url:
                video_info['hls_url'] = hls_url
            else:
                print(f"  警告: 未找到HLS链接")

        video_info['scraped_at'] = datetime.now().isoformat()
        video_info['scraped_date'] = date.today().isoformat()

        videos.append(video_info)

        if idx < total_videos:
            time.sleep(1)

    return videos


def main():
    conn = None
    try:
        conn = get_db_connection()
        init_db(conn)

        videos = scrape_jable_videos()

        if not videos:
            print("错误: 未找到任何视频数据")
            sys.exit(1)

        success_count, fail_count = save_videos_to_db(conn, videos)

        print("\n" + "=" * 60)
        print("抓取完成!")
        print(f"总计抓取: {len(videos)} 个视频")
        print(f"有HLS链接的视频: {sum(1 for v in videos if v.get('hls_url'))}")
        print(f"数据库写入成功: {success_count} 条")
        print(f"数据库写入失败: {fail_count} 条")
        print("=" * 60)

    except Exception as e:
        print(f"抓取过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")


if __name__ == '__main__':
    main()
