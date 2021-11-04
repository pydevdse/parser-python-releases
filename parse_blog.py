import datetime
from random import randint
import requests
from lxml import html
from firefox_ua import USER_AGENT
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from models import Base, Post, Release, FilesTable, Author

engine = create_engine('sqlite:///releases.db')#, echo=True)
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)
db_session = Session()

Base.metadata.create_all(engine)


class Parser():
    def get_session(self, url):
        session = requests.session()
        #session.verify = False
        session.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US;q=0.8,en;q=0.5',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': url.split('//')[-1].split('/')[0],
            'User-Agent' : USER_AGENT[randint(0, len(USER_AGENT)-1)]
        }
        return session
    # ----------- Parsing "Files" Table Release and save to base------------------
    def get_table_release(self, table, release_id): 
        for line in table:
            l = line.xpath('.//td')
            if not l or len(l)<5:
                print('TABLE LINE NOT FOUND:', line.text)
                continue
            try:
                version = l[0].xpath('./a/text()')[0]
            except Exception as e:
                print('ERROR', e)
                print('Field "Version" in line table not found, please contact developer')
                version = ''
            try:
                url_tgz = l[0].xpath('./a/@href')[0]
            except Exception as e:
                print('ERROR', e)
                print('Field "Href" in line table not found, please contact developer')
                url_tgz = ''
            try:
                oper_syst = l[1].xpath('./text()')[0]
            except Exception as e:
                print('ERROR', e)
                print('Field "Operating System" in line table not found, please contact developer')
                oper_syst = ''
            try:
                description = l[2].xpath('./text()')[0]
            except Exception as e:
                print(e)
                description = ''
                print('Field "Description" in line table not found, please contact developer')
            try:
                md5_sum = l[3].xpath('./text()')[0]
            except Exception as e:
                print('ERROR', e)
                print('Field "md5 sum" in line table not found, please contact developer')
                md5_sum = 0
            try:
                file_size = l[4].xpath('./text()')[0]
            except Exception as e:
                print('ERROR', e)
                print('Field "file size" in line table not found, please contact developer')
                file_size = 0
                
            files_table = FilesTable(version_name=version, url_tgz=url_tgz, operating_system=oper_syst,
                description=description, md5_sum=md5_sum, file_size=file_size, release_id=release_id)
            db_session.add(files_table)
            db_session.commit()

    def get_release(self, url, post_id, proxy=False):
        session = self.get_session(url)
        session.cookies.clear()
        page = session.get(url)
        tree = html.fromstring(page.content)
        try:
            title = tree.xpath('//meta[@property="og:title"]/@content')[0]
        except Exception as e:
            print('Error:', e)
            print('Field release_title not found, please contact to developer')
            title = None
        try:
            release_name = tree.xpath('.//header[@class="article-header"]/h1[@class="page-title"]/text()')[0]
        except Exception as e:
            print('Error:', e)
            print('Field release_name not found, please contact to developer')
            release_name = None
        try:
            release_date = tree.xpath('.//article[@class="text"]//p/text()')[0]
        except Exception as e:
            print('ERROR:', e)
            print('Field release_date not found, please contact to developer')
            release_date = None
        try:
            text = tree.xpath('.//article[@class="text"]')[0]
        except Exception as e:
            print('Error:', e)
            print('Field release_text not found, please contact to developer')
            text = None
        text_article = ''
        for t in text.findall('*'):
            if t.tag == 'ul':
                ul = t.xpath('.//li//text()')
                ul_text = ''
                for u in ul:
                    ul_text += u
                text_article += ul_text+'\n'
                continue
            te = t.xpath('.//text()')
            if not te:
                continue
            if 'Files' in te and t.tag == 'header':
                break
            text_p = ''
            for te_p in te:
                text_p += te_p
            text_article += text_p+'\n'
        peps = tree.xpath('.//a/@href')
        peps_urls = {'urls': [p for p in peps if '/peps/pep-' in p]}
        release = Release(name=release_name, title=title, date=release_date, url=url, urls_pep=peps_urls,
                              text=text_article, post_id=post_id)
        db_session.add(release)
        db_session.commit()
        table = tree.xpath('.//table/tbody/tr')
        if not table:
            print(f"Table not found in release name: {release.name}, release ID {release.id}")
        else:
            self.get_table_release(table, release.id)
        session.cookies.clear()
        session.close()
        return release.id

    def parse_posts(self, url):
        session = self.get_session(url)
        page = session.get(url, timeout=10)
        tree = html.fromstring(page.content)
        list_posts = tree.xpath('//div[@class="date-outer"]')
        for l in list_posts:
            print('---------------------------------')
            text = l.xpath('.//text()')
            text_page = ''
            for t in text:
                if ('Get it here' in t) or ('http' in t):
                    continue
                text_page += t
            try:
                post_title = l.xpath('.//h3[@class="post-title entry-title"]/a/text()')[0]
            except Exception as e:
                print('Error:', e)
                print('Please contact to developer, post_title not found')
                post_title = None
            try:
                author_post = l.xpath('.//div[@class="post-footer"]//span[@class="fn"]/text()')[0]
            except Exception as e:
                print('Error:', e)
                print('Please contact to developer, author_post not found')
                author_post = None
            try:
                date_post = l.xpath('.//div[@class="post-footer"]//span[@class="post-timestamp"]/a/abbr/@title')[0].split('T')[0]
                date_post = date_post.split('-')
                date_post = datetime.date(int(date_post[0]), int(date_post[1]), int(date_post[2]))
            except Exception as e:
                print('Error:', e)
                print('Please contact to developer, date_post not found')
                date_post = None
            urls = l.xpath('.//a/@href')
            urls_release = [u for u in urls if 'downloads/release' in u]
            post = Post()
            try:
                author = db_session.query(Author).filter(Author.name == author_post).one()
            except NoResultFound:
                author = Author(name=author_post)
                db_session.add(author)
                db_session.commit()
                print('Author not found')
                print('Create new Author', author.id)
            post.author_id = author.id
            post.title = post_title
            post.text_post = text_page
            post.date = date_post
            db_session.add(post)
            db_session.commit()
            print('Post ID:', post.id)
            for u in urls_release:
                release_id = self.get_release(u, post.id)

        try:
            next_page_url = tree.xpath('//a[@class="blog-pager-older-link"]/@href')[0]
        except Exception as e:
            print('Error:', e)
            print('Please contact to developer, next page not found')
            next_page_url = False

        session.cookies.clear()
        session.close()
        return next_page_url

    def main(self, url, PAGES = 1):
        i=0
        next_page_url = self.parse_posts(url)
        while i<PAGES:
            i += 1
            if next_page_url:
                next_page_url = self.parse_posts(next_page_url)
            else: # страницы постов закончились.
                break
        author = db_session.query(Author).get(1)
        author_posts = db_session.query(Post).filter(Post.author_id==author.id)
        print(f"Author {author.name} posts:", author_posts.count())

if __name__ == '__main__':
    url = 'https://blog.python.org/'
    p = Parser()
    pages = 2  # кол-во страниц постов
    p.main(url, pages)
    db_session.close()
