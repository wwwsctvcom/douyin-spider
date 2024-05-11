import time
import requests
from loguru import logger


class DouYinCrawler:

    def __init__(self):
        self.headers = {
            'User-Agent': "",
            'Cookie': "",  # 必须，登录网页版获取cookie
            'Referer': 'https://www.douyin.com/',
            'Connection': 'keep-alive'
        }

    @staticmethod
    def int_to_strftime(a):
        b = time.localtime(a)  # 转为日期字符串
        c = time.strftime("%Y-%m-%d %H:%M:%S", b)  # 格式化字符串
        return c

    def get_searched_video_link(self, query: str = None, max_video_num: int = None) -> list:
        """获取抖音搜索之后的video link"""
        searched_urls = []

        base_url = "https://www.douyin.com/aweme/v1/web/search/item"

        params = {
            'aid': 6383,
            'keyword': query,
            'offset': 0,
            'count': max_video_num,
        }

        try:
            response = requests.get(base_url, headers=self.headers, params=params, verify=False, timeout=10)
            for video_info in response.json()["data"]:
                searched_urls.append({"aweme_id": video_info['aweme_info']['aweme_id'],
                                      "desc": video_info['aweme_info']["desc"],
                                      "comment_count": video_info['aweme_info']["statistics"]["comment_count"]})
        except Exception as e:
            logger.error(e)
        return searched_urls

    def get_comments(self, aweme_id: str = None, comment_count: int = None):
        # https://www.douyin.com/video/7361443223765519651
        response = None
        parent_comments = []

        base_url = "https://www.douyin.com/aweme/v1/web/comment/list/"

        params = {
            'aid': 6383,
            'aweme_id': aweme_id,
            'cursor': 0,
            'count': comment_count,
        }

        try:
            response = requests.get(base_url, headers=self.headers, params=params, verify=False, timeout=10)
            if response.status_code != 200:
                return parent_comments
        except Exception as e:
            logger.error(f"access to {response.url} failed, ", e)

        for comment in response.json()["comments"]:
            try:
                parent_comments.append({'cid': comment["cid"],
                                        'text': comment["text"],
                                        'aweme_id': comment["aweme_id"],
                                        'create_time': self.int_to_strftime(comment["create_time"]),
                                        'reply_comment_total': int(comment["reply_comment_total"])})
            except Exception as e:
                logger.error(f"parsing {comment} failed, ", e)
        return parent_comments

    def get_reply_comments(self,
                       aweme_id: str = None,
                       root_comment_cid: str = None,
                       reply_comment_total: int = None):
        """每个request最多每次获取50个子评论"""
        response = None
        reply_comments = []

        base_url = "https://www.douyin.com/aweme/v1/web/comment/list/reply"

        cursor = 0
        count = 50
        while True:
            params = {
                "aid": 6383,
                "item_id": aweme_id,  # parent comment aweme_id
                "comment_id": root_comment_cid,  # parent cid
                "cursor": cursor,
                "count": count
            }

            try:
                response = requests.get(base_url, headers=self.headers, params=params, verify=False, timeout=10)
                if response.status_code != 200:
                    return reply_comments
            except Exception as e:
                logger.error(f"access to {response.url} failed, ", e)

            for comment in response.json()["comments"]:
                try:
                    reply_comments.append({'cid': comment["cid"],
                                           'text': comment["text"],
                                           'aweme_id': comment["aweme_id"],
                                           'create_time': self.int_to_strftime(comment["create_time"]),
                                           'reply_comment_total': 0})
                except Exception as e:
                    logger.error(f"parsing {comment} failed, ", e)

            cursor += 50
            if cursor >= reply_comment_total:
                break
        return reply_comments

    def _start_crawl(self, aweme_id: str = None, comment_count: int = None):
        parent_comments = self.get_comments(aweme_id=aweme_id, comment_count=comment_count)
        for parent_comment in parent_comments:
            parent_cid = parent_comment["cid"]
            parent_text = parent_comment["text"]
            parent_create_time = parent_comment["create_time"]
            parent_reply_comment_total = parent_comment["reply_comment_total"]

            print("Parent Comment: ", parent_cid, parent_text, parent_create_time)
            if parent_reply_comment_total > 0:
                reply_comments = self.get_reply_comments(aweme_id=aweme_id,
                                                         root_comment_cid=parent_cid,
                                                         reply_comment_total=parent_reply_comment_total)
                for reply_comment in reply_comments:
                    reply_cid = reply_comment["cid"]
                    reply_text = reply_comment["text"]
                    reply_create_time = reply_comment["create_time"]
                    print("Reply Comment: ", reply_cid, reply_text, reply_create_time)

    def start_crawl(self, query: str = None, max_video_num: int = None):
        videos_info = self.get_searched_video_link(query, max_video_num)
        for video_info in videos_info:
            aweme_id = video_info["aweme_id"]
            comment_count = video_info["comment_count"]
            logger.info(f"Start crawl https://www.douyin.com/video/{aweme_id}, comment count: {comment_count}")

            self._start_crawl(aweme_id=aweme_id, comment_count=comment_count)
            break


if __name__ == "__main__":
    douyin_crawler = DouYinCrawler()
    douyin_crawler.start_crawl("百度地图", 10)
