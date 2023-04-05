import json
import boto3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from wordfilter import Wordfilter
import praw

load_dotenv()

client_id = os.environ.get("REDDIT_CLIENT_ID")
client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
password = os.environ.get("REDDIT_PASSWORD")
username = os.environ.get("REDDIT_USERNAME")
user_agent = os.environ.get("REDDIT_USER_AGENT")

print('Loading function')

s3 = boto3.client('s3')

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    password=password,
    username=username,
    user_agent=user_agent
)

wordfilter = Wordfilter()
vulgarities = ["a55", "a55hole", "aeolus", "ahole", "anal", "analprobe", "anilingus", "anus", "areola", "areole", "arian", "aryan", "ass", "assbang", "assbanged", "assbangs", "asses", "assfuck", "assfucker", "assh0le", "asshat", "assho1e", "ass hole", "assholes", "assmaster", "assmunch", "asswipe", "asswipes", "azazel", "azz", "b1tch", "babe", "babes", "ballsack", "bang", "banger", "barf", "bastard", "bastards", "bawdy", "beaner", "beardedclam", "beastiality", "beatch", "beater", "beaver", "beer", "beeyotch", "beotch", "biatch", "bigtits", "big tits", "bimbo", "bitch", "bitched", "bitches", "bitchy", "blow job", "blow", "blowjob", "blowjobs", "bod", "bodily", "boink", "bollock", "bollocks", "bollok", "bone", "boned", "boner", "boners", "bong", "boob", "boobies", "boobs", "booby", "booger", "bookie", "bootee", "bootie", "booty", "booze", "boozer", "boozy", "bosom", "bosomy", "bowel", "bowels", "bra", "brassiere", "breast", "breasts", "bugger", "bukkake", "bullshit", "bull shit", "bullshits", "bullshitted", "bullturds", "bung", "busty", "butt", "butt fuck", "buttfuck", "buttfucker", "buttfucker", "buttplug", "c.0.c.k", "c.o.c.k.", "c.u.n.t", "c0ck", "c-0-c-k", "caca", "cahone", "cameltoe", "carpetmuncher", "cawk", "cervix", "chinc", "chincs", "chink", "chink", "chode", "chodes", "cl1t", "climax", "clit", "clitoris", "clitorus", "clits", "clitty", "cocain", "cocaine", "cock", "c-o-c-k", "cockblock", "cockholster", "cockknocker", "cocks", "cocksmoker", "cocksucker", "cock sucker", "coital", "commie", "condom", "coon", "coons", "corksucker", "crabs", "crack", "cracker", "crackwhore", "crap", "crappy", "cum", "cummin", "cumming", "cumshot", "cumshots", "cumslut", "cumstain", "cunilingus", "cunnilingus", "cunny", "cunt", "cunt", "c-u-n-t", "cuntface", "cunthunter", "cuntlick", "cuntlicker", "cunts", "d0ng", "d0uch3", "d0uche", "d1ck", "d1ld0", "d1ldo", "dago", "dagos", "dammit", "damn", "damned", "damnit", "dawgie-style", "dick", "dickbag", "dickdipper", "dickface", "dickflipper", "dickhead", "dickheads", "dickish", "dick-ish", "dickripper", "dicksipper", "dickweed", "dickwhipper", "dickzipper", "diddle", "dike", "dildo", "dildos", "diligaf", "dillweed", "dimwit", "dingle", "dipship", "doggie-style", "doggy-style", "dong", "doofus", "doosh", "dopey", "douch3", "douche", "douchebag", "douchebags", "douchey", "drunk", "dumass", "dumbass", "dumbasses", "dummy", "dyke", "dykes", "ejaculate", "enlargement", "erect", "erection", "erotic", "essohbee", "extacy", "extasy", "f.u.c.k", "fack", "fag", "fagg", "fagged", "faggit", "faggot", "fagot", "fags", "faig", "faigt", "fannybandit", "fart", "fartknocker", "fat", "felch", "felcher", "felching", "fellate", "fellatio", "feltch", "feltcher", "fisted", "fisting", "fisty", "floozy", "foad", "fondle", "foobar", "foreskin", "freex", "frigg", "frigga", "fubar", "fuck", "f-u-c-k", "fuckass", "fucked", "fucked", "fucker", "fuckface", "fuckin", "fucking", "fucknugget", "fucknut", "fuckoff", "fucks", "fucktard", "fuck-tard", "fuckup", "fuckwad", "fuckwit", "fudgepacker", "fuk", "fvck", "fxck", "gae", "gai", "ganja", "gay", "gays", "gey", "gfy", "ghay", "ghey", "gigolo", "glans", "goatse", "godamn", "godamnit", "goddam", "goddammit", "goddamn", "goldenshower", "gonad", "gonads", "gook", "gooks", "gringo", "gspot", "g-spot", "gtfo", "guido", "h0m0", "h0mo", "handjob", "hard on", "he11", "hebe", "heeb", "hell", "hemp", "heroin", "herp", "herpes", "herpy", "hitler", "hiv", "hobag", "hom0", "homey", "homo", "homoey", "honky", "hooch", "hookah", "hooker", "hoor", "hootch", "hooter", "hooters", "horny", "hump", "humped", "humping", "hussy", "hymen", "inbred", "incest", "injun", "j3rk0ff", "jackass", "jackhole", "jackoff", "jap", "japs", "jerk", "jerk0ff", "jerked", "jerkoff", "jism", "jiz", "jizm", "jizz", "jizzed", "junkie", "junky", "kike", "kikes", "kill", "kinky", "kkk", "klan", "knobend", "kooch", "kooches", "kootch", "kraut", "kyke", "labia", "lech", "leper", "lesbians", "lesbo", "lesbos", "lez", "lezbian", "lezbians", "lezbo", "lezbos", "lezzie", "lezzies", "lezzy", "lmao", "lmfao", "loin", "loins", "lube", "lusty", "mams", "massa", "masterbate", "masterbating", "masterbation", "masturbate", "masturbating", "masturbation", "maxi", "menses", "menstruate", "menstruation", "meth", "m-fucking", "mofo", "molest", "moolie", "moron", "motherfucka", "motherfucker", "motherfucking", "mtherfucker", "mthrfucker", "mthrfucking", "muff", "muffdiver", "murder", "muthafuckaz", "muthafucker", "mutherfucker", "mutherfucking", "muthrfucking", "nad", "nads", "naked", "napalm", "nappy", "nazi", "nazism", "negro", "nigga", "niggah", "niggas", "niggaz", "nigger", "nigger", "niggers", "niggle", "niglet", "nimrod", "ninny", "nipple", "nooky", "nympho", "opiate", "opium", "oral", "orally", "organ", "orgasm", "orgasmic", "orgies", "orgy", "ovary", "ovum", "ovums", "p.u.s.s.y.", "paddy", "paki", "pantie", "panties", "panty", "pastie", "pasty", "pcp", "pecker", "pedo", "pedophile", "pedophilia", "pedophiliac", "pee", "peepee", "penetrate", "penetration", "penial", "penile", "penis", "perversion", "peyote", "phalli", "phallic", "phuck", "pillowbiter", "pimp", "pinko", "piss", "pissed", "pissoff", "piss-off", "pms", "polack", "pollock", "poon", "poontang", "porn", "porno", "pornography", "pot", "potty", "prick", "prig", "prostitute", "prude", "pube", "pubic", "pubis", "punkass", "punky", "puss", "pussies", "pussy", "pussypounder", "puto", "queaf", "queef", "queef", "queer", "queero", "queers", "quicky", "quim", "racy", "rape", "raped", "raper", "rapist", "raunch", "rectal", "rectum", "rectus", "reefer", "reetard", "reich", "retard", "retarded", "revue", "rimjob", "ritard", "rtard", "r-tard", "rum", "rump", "rumprammer", "ruski", "s.h.i.t.", "s.o.b.", "s0b", "sadism", "sadist", "scag", "scantily", "schizo", "schlong", "screw", "screwed", "scrog", "scrot", "scrote", "scrotum", "scrud", "scum", "seaman", "seamen", "seduce", "semen", "sex", "sexual", "sh1t", "s-h-1-t", "shamedame", "shit", "s-h-i-t", "shite", "shiteater", "shitface", "shithead", "shithole", "shithouse", "shits", "shitt", "shitted", "shitter", "shitty", "shiz", "sissy", "skag", "skank", "slave", "sleaze", "sleazy", "slut", "slutdumper", "slutkiss", "sluts", "smegma", "smut", "smutty", "snatch", "sniper", "snuff", "s-o-b", "sodom", "souse", "soused", "sperm", "spic", "spick", "spik", "spiks", "spooge", "spunk", "steamy", "stfu", "stiffy", "stoned", "strip", "stroke", "stupid", "suck", "sucked", "sucking", "sumofabiatch", "t1t", "tampon", "tard", "tawdry", "teabagging", "teat", "terd", "teste", "testee", "testes", "testicle", "testis", "thrust", "thug", "tinkle", "tit", "titfuck", "titi", "tits", "tittiefucker", "titties", "titty", "tittyfuck", "tittyfucker", "toke", "toots", "tramp", "transsexual", "trashy", "tubgirl", "turd", "tush", "twat", "twats", "ugly", "undies", "unwed", "urinal", "urine", "uterus", "uzi", "vag", "vagina", "valium", "viagra", "virgin", "vixen", "vodka", "vomit", "voyeur", "vulgar", "vulva", "wad", "wang", "wank", "wanker", "wazoo", "wedgie", "weed", "weenie", "weewee", "weiner", "weirdo", "wench", "wetback", "wh0re", "wh0reface", "whitey", "whiz", "whoralicious", "whore", "whorealicious", "whored", "whoreface", "whorehopper", "whorehouse", "whores", "whoring", "wigger", "womb", "woody", "wop", "wtf", "x-rated", "xxx", "yeasty", "yobbo", "zoophile"]
wordfilter.addWords(vulgarities)

def lambda_handler(event, context):
    # Set the start and end dates for the search (in UTC timezone)

    # post_date = datetime.utcnow()
    time_stamp = datetime.utcnow().replace(second=0, microsecond=0)
    crawl_day = (datetime.utcnow() - timedelta(days=2)).strftime("%d-%m-%Y") # dd-mm-yyyy
    recrawl_day = time_stamp.strftime("%d-%m-%Y")
    bucket="tf-is459-ukraine-war-data"
    obj = s3.get_object(Bucket=bucket, Key='topics.txt')
    topics = obj['Body'].read().decode("utf-8").split("\n")
    
    for query in topics:
        updated_posts = []
        posts = []
        comments = []
        bucket_search_content = s3.list_objects_v2(Bucket=bucket, Prefix=f"reddit_initial/topic={query}/dataload={crawl_day}/").get('Contents', [])

        file_keys = [file['Key'] for file in bucket_search_content]
        for key in file_keys:
            post_file = s3.get_object(Bucket=bucket, Key=key)
            file_content = json.loads((post_file['Body'].read().decode("utf-8")))
            posts.extend(file_content)
    
        posts_key=f"project/reddit_posts/topic={query}/dataload={recrawl_day}/{time_stamp}_posts_aggregated.json"
        comments_key=f"project/reddit_comments/topic={query}/dataload={recrawl_day}/{time_stamp}_comments.json"

        for post in posts:
            try:
                updated_post = reddit.submission(id=post['id'])
                updated_posts.append({
                    'id':str(updated_post.id),
                    'date': str(datetime.fromtimestamp(updated_post.created_utc)),
                    'title':str(updated_post.title),
                    'content':str(updated_post.selftext),
                    'username':str(updated_post.author),
                    'subreddit':str(updated_post.subreddit),
                    'commentCount': int(updated_post.num_comments),
                    'score': int(updated_post.score),
                })
                if updated_post.num_comments > 0:
                    submission = reddit.submission(id=post['id'])
                    submission.comments.replace_more(limit=None)
                    for comment in submission.comments.list():
                        if str(comment.author) == "AutoModerator":
                            continue
                        if comment.author == None:
                            continue
                        if len(str(comment.body)) > 1000:
                            continue
                        if wordfilter.blacklisted(str(comment.body)):
                            continue
                        comments.append({
                            'id': str(comment.id),
                            'date': str(datetime.fromtimestamp(comment.created_utc)),
                            'content': str(comment.body),
                            'username': str(comment.author.name),
                            'score': int(comment.score),
                            'post_id': str(updated_post.id),
                            'parent_id': str(comment.parent_id),
                        })
            except Exception as e:
                print(e)
            
        posts_json = json.dumps(updated_posts)
        comments_json = json.dumps(comments)
        response1 = s3.put_object(Body=posts_json, Bucket=bucket, Key=posts_key)
        response2 = s3.put_object(Body=comments_json, Bucket=bucket, Key=comments_key)        
    return response1, response2 
