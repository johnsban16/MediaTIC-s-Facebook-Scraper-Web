import sys
import os
import copy
import csv
import datetime
import calendar
import zipfile
import json
import socket
import time
import urllib.request

socket.setdefaulttimeout(30)


def load_data(data, enc='utf-8'):
    if type(data) is str:
        csv_data = []
        with open(data, 'r', encoding=enc, errors='replace') as f:
            reader = csv.reader((line.replace('\0', '') for line in f))  # remove NULL bytes
            for row in reader:
                if row != []:
                    csv_data.append(row)
        return csv_data
    else:
        return copy.deepcopy(data)


def save_csv(filename, data, use_quotes=True, file_mode='w',
             enc='utf-8'):  # this assumes a list of lists wherein the second-level list items contain no commas
    with open(filename, file_mode, encoding=enc) as out:
        for line in data:
            if use_quotes == True:
                row = '"' + '","'.join([str(i).replace('"', "'") for i in line]) + '"' + "\n"
            else:
                row = ','.join([str(i) for i in line]) + "\n"
            out.write(row)


def url_retry(url):
    succ = 0
    while succ == 0:
        try:
            json_out = json.loads(urllib.request.urlopen(url).read().decode(encoding="utf-8"))
            succ = 1
        except Exception as e:
            print(str(e))
            if 'HTTP Error 4' in str(e):
                return False
            else:
                time.sleep(1)
    return json_out


def optional_field(dict_item, dict_key):
    try:
        out = dict_item[dict_key]
        if dict_key == 'shares':
            out = dict_item[dict_key]['count']
        if dict_key == 'likes':
            out = dict_item[dict_key]['summary']['total_count']
    except KeyError:
        out = ''
    return out


def make_csv_chunk(fb_json_page, scrape_mode, thread_starter='', msg=''):
    csv_chunk = []
    if scrape_mode == 'feed' or scrape_mode == 'posts':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
                        '_' + line['from']['id'], \
                        optional_field(line, 'message'), \
                        optional_field(line, 'picture'), \
                        optional_field(line, 'link'), \
                        optional_field(line, 'name'), \
                        optional_field(line, 'description'), \
                        optional_field(line, 'type'), \
                        line['created_time'], \
                        optional_field(line, 'shares'), \
                        optional_field(line, 'likes'), \
                        optional_field(line, 'LOVE'), \
                        optional_field(line, 'WOW'), \
                        optional_field(line, 'HAHA'), \
                        optional_field(line, 'SAD'), \
                        optional_field(line, 'ANGRY'), \
                        line['id']]
            csv_chunk.append(csv_line)
    if scrape_mode == 'comments':
        for line in fb_json_page['data']:
            csv_line = [line['from']['name'], \
                        '_' + line['from']['id'], \
                        optional_field(line, 'message'), \
                        line['created_time'], \
                        optional_field(line, 'like_count'), \
                        line['id'], \
                        thread_starter, \
                        msg]
            csv_chunk.append(csv_line)

    return csv_chunk


'''
# The first five fields of scrape_fb are fairly self-explanatory or are explained above. 
# scrape_mode can take three values: "feed," "posts," or "comments." The first two are identical in most cases and pull the main posts from a public wall. "comments" pulls the comments from a given permalink for a post. Only use "comments" if your IDs are post permalinks.
# You can use end_date to specify a date around which you'd like the program to stop. It won't stop exactly on that date, but rather a little after it. If present, it needs to be a string in yyyy-mm-dd format. If you leave the field blank, it will extract all available data. 
'''


def scrape_fb(client_id, client_secret, ids, outfile="fb_data.csv", version="2.7", scrape_mode="feed", end_date=""):
    time1 = time.time()
    if type(client_id) is int:
        client_id = str(client_id)
    fb_urlobj = urllib.request.urlopen(
        'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    if "," in ids:
        fb_ids = [i.strip() for i in ids.split(",")]
    elif '.csv' in ids or '.txt' in ids:
        fb_ids = [i[0].strip() for i in load_data(ids)]
    else:
        fb_ids = [ids]

    try:
        end_dateobj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        end_dateobj = ''

    if scrape_mode == 'feed' or scrape_mode == 'posts':
        header = ['from', 'from_id', 'message', 'picture', 'link', 'name', 'description', 'type', 'created_time',
                  'shares', 'likes', 'loves', 'wows', 'hahas', 'sads', 'angrys', 'post_id']
    else:
        header = ['from', 'from_id', 'comment', 'created_time', 'likes', 'post_id', 'original_poster',
                  'original_message']

    csv_data = []
    csv_data.insert(0, header)
    save_csv(outfile, csv_data, file_mode="a")

    for x, fid in enumerate(fb_ids):
        if scrape_mode == 'comments':
            msg_url = 'https://graph.facebook.com/v' + version + '/' + fid + '?fields=from,message&' + fb_token
            msg_json = url_retry(msg_url)
            if msg_json == False:
                print("URL not available. Continuing...", fid)
                continue
            msg_user = msg_json['from']['name']
            msg_content = optional_field(msg_json, 'message')
            field_list = 'from,message,created_time,like_count'
        else:
            msg_user = ''
            msg_content = ''
            field_list = 'from,message,picture,link,name,description,type,created_time,shares,likes.summary(total_count).limit(0)'

        data_url = 'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=' + field_list + '&limit=100&' + fb_token

        # sys.exit()
        data_rxns = []
        new_rxns = ['LOVE', 'WOW', 'HAHA', 'SAD', 'ANGRY']
        for i in new_rxns:
            data_rxns.append(
                'https://graph.facebook.com/v' + version + '/' + fid.strip() + '/' + scrape_mode + '?fields=reactions.type(' + i + ').summary(total_count).limit(0)&limit=100&' + fb_token)

        next_item = url_retry(data_url)
        # with open("Output.txt", "w") as text_file:
        # print(next_item, file=text_file)



        if next_item != False:
            for n, i in enumerate(data_rxns):
                tmp_data = url_retry(i)
                for z, j in enumerate(next_item['data']):
                    try:
                        j[new_rxns[n]] = tmp_data['data'][z]['reactions']['summary']['total_count']
                    except (KeyError, IndexError):
                        j[new_rxns[n]] = 0

            csv_data = make_csv_chunk(next_item, scrape_mode, msg_user, msg_content)
            save_csv(outfile, csv_data, file_mode="a")
        else:
            print("Skipping ID " + fid + " ...")
            continue
        n = 0

        while 'paging' in next_item and 'next' in next_item['paging']:
            next_item = url_retry(next_item['paging']['next'])
            try:
                for i in new_rxns:
                    start = next_item['paging']['next'].find("from")
                    end = next_item['paging']['next'].find("&", start)
                    next_rxn_url = next_item['paging']['next'][
                                   :start] + 'reactions.type(' + i + ').summary(total_count).limit(0)' + \
                                   next_item['paging']['next'][end:]
                    tmp_data = url_retry(next_rxn_url)
                    for z, j in enumerate(next_item['data']):
                        try:
                            j[i] = tmp_data['data'][z]['reactions']['summary']['total_count']
                        except (KeyError, IndexError):
                            j[i] = 0
            except KeyError:
                continue

            csv_data = make_csv_chunk(next_item, scrape_mode, msg_user, msg_content)
            save_csv(outfile, csv_data, file_mode="a")
            try:
                print(n + 1, "page(s) of data archived for ID", fid, "at", next_item['data'][-1]['created_time'], ".",
                      round(time.time() - time1, 2), 'seconds elapsed.')
            except IndexError:
                break
            n += 1
            time.sleep(1)
            if end_dateobj != '' and end_dateobj > datetime.datetime.strptime(
                    next_item['data'][-1]['created_time'][:10], "%Y-%m-%d").date():
                break

        print(x + 1, 'Facebook ID(s) archived.', round(time.time() - time1, 2), 'seconds elapsed.')

    print('Script completed in', time.time() - time1, 'seconds.')
    return csv_data


#Métodos propios------------------------------------------------------------------------

def getAccessToken(client_id, client_secret):
    fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + client_id + '&client_secret=' + client_secret)
    fb_token = 'access_token=' + json.loads(fb_urlobj.read().decode(encoding="latin1"))['access_token']
    return fb_token

def addCommentsAndRepliesToCSV(comments, nodeoutfile, edgeoutfile):
    for comment in comments['data']:
        parent_comment_id = [ comment['id'],'comment',comment['message'], comment['created_time']]
        csv_data = []
        csv_data.insert(0, parent_comment_id)
        save_csv(nodeoutfile, csv_data, file_mode="a")
        if 'comments' in comment:
            for reply in comment['comments']['data']:
                list_of_user_in_reply = []
                if reply['from']['id'] not in list_of_user_in_reply:
                    list_of_user_in_reply.append(reply['from']['id'])
                    reply_id = [reply['from']['id'], 'reply', reply['message'], reply['created_time']]
                    csv_data = []
                    csv_data.insert(0, reply_id)
                    save_csv(nodeoutfile, csv_data, file_mode="a")
                    #insertar las aristas
                    edge = [reply['from']['id'], comment['id']]
                    csv_data = []
                    csv_data.insert(0, edge)
                    save_csv(edgeoutfile, csv_data, file_mode="a")

def addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges):

    post_reaction_count = post['like']['summary']['total_count'] + post['love']['summary']['total_count'] + \
                          post['wow']['summary']['total_count'] + post['haha']['summary']['total_count'] + \
                          post['sad']['summary']['total_count'] + post['angry']['summary']['total_count']
    post_share_count = post['shares']['count'] if 'shares' in post else 0
    post_comments = 0
    if 'comments' in post:
        post_comments = len(post['comments']['data'])
    post_engagement = post_share_count + post_reaction_count + post_comments
    list_posts = [post['id'], post['type'], optional_field(post,'link'), optional_field(post,'name'),
                    optional_field(post,'message'), post['created_time'],
                    optional_field(post,'shares'), optional_field(post, 'likes'), post_comments, post_reaction_count, post_engagement]
    csv_data = []
    csv_data.insert(0, list_posts)
    save_csv(outfile_nodes, csv_data, file_mode="a")

    if 'comments' in post:
        for comment in post['comments']['data']:
            list_of_user_in_post = []
            if comment['from']['id'] not in list_of_user_in_post:
                list_of_user_in_post.append(comment['from']['id'])
                list_comment_id = [comment['from']['id'], 'user' ,'', '', 
                                    comment['message'], comment['created_time']]
                csv_data = []
                csv_data.insert(0, list_comment_id)
                save_csv(outfile_nodes, csv_data, file_mode="a")
                # insertar las aristas
                edge = [comment['from']['id'], post['id']]
                csv_data = []
                csv_data.insert(0, edge)
                save_csv(outfile_edges, csv_data, file_mode="a")

def addPostsAndReactionsToCSV(post, outfile_nodes, outfile_edges):

    reaction_types = ['LIKE', 'LOVE', 'HAHA', 'WOW', 'SAD', 'ANGER']
    for reaction in reaction_types:
        list_posts = [post['id'], post['type'], '', optional_field(post, 'link'), optional_field(post, 'name'),
                      optional_field(post, 'message'), post['created_time']]
        csv_data = []
        csv_data.insert(0, list_posts)
        save_csv(outfile_nodes + '_' + reaction + '.csv', csv_data, file_mode="a")

    if 'reactions' in post:
        for reaction in post['reactions']['data']:
            list_reactions_id = [reaction['id'], 'user', reaction['type'] , '', '', '', '']
            csv_data = []
            csv_data.insert(0, list_reactions_id)
            save_csv(outfile_nodes + '_' + reaction['type'] + '.csv', csv_data, file_mode="a")
            # insertar las aristas
            csv_data = []
            edge = [reaction['id'], post['id']]
            csv_data.insert(0, edge)
            save_csv(outfile_edges + '_' + reaction['type'] + '.csv', csv_data, file_mode="a")

        while 'paging' in post['reactions'] and 'next' in post['reactions']['paging']:
            post['reactions'] = url_retry(post['reactions']['paging']['next'])
            for reaction in post['reactions']['data']:
                list_reactions_id = [reaction['id'], 'user', reaction['type'], '', '', '', '']
                csv_data = []
                csv_data.insert(0, list_reactions_id)
                save_csv(outfile_nodes + '_' + reaction['type'] + '.csv', csv_data, file_mode="a")
                # insertar las aristas
                edge = [reaction['id'], post['id']]
                csv_data = []
                csv_data.insert(0, edge)
                save_csv(outfile_edges + '_' + reaction['type'] + '.csv', csv_data, file_mode="a")


def buildCommentsCSVs(client_id, client_secret, site_id, since, until, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    field_list = 'id,message,created_time,comments{id,message,from,created_time,comments{id,message,from,created_time}}'
    #data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '?fields=posts{' + field_list + '}&limit=100&' + fb_token

    #&since='+since+'&until='+ until
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+str(since)+'&until='+str(until)+'&' + fb_token
    next_item = url_retry(data_url)

    # set CSV headers
    headerNodeFile = ['node_id', 'type', 'message', 'created_time']
    csv_data = []
    csv_data.insert(0, headerNodeFile)
    save_csv(outfile_nodes, csv_data, file_mode="a")

    headerEdgeFile = ['source', 'target']
    csv_data = []
    csv_data.insert(0, headerEdgeFile)
    save_csv(outfile_edges, csv_data, file_mode="a")

    for post in next_item['data']:
        if 'comments' in post:
            addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges )

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            if 'comments' in post:
                addCommentsAndRepliesToCSV(post['comments'], outfile_nodes, outfile_edges)



def buildPostCSVs(client_id, client_secret, site_id, since, until, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    reaction_count_queries = 'reactions.type(LIKE).limit(0).summary(1).as(like),reactions.type(WOW).limit(0).summary(1).as(wow),' \
                             'reactions.type(SAD).limit(0).summary(1).as(sad),reactions.type(HAHA).limit(0).summary(1).as(haha),' \
                             'reactions.type(LOVE).limit(0).summary(1).as(love),reactions.type(ANGRY).limit(0).summary(1).as(angry)'
    field_list = 'id,comments{id,from,message,created_time},message,link,name,type,created_time,shares,likes.summary(total_count).limit(0),' + reaction_count_queries
    #data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '?fields=posts{' + field_list + '}&limit=100&' + fb_token
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+str(since)+'&until='+str(until)+'&' + fb_token
    next_item = url_retry(data_url)

    headerNodeFile = ['node_id', 'type', 'link', 'name','message', 'created_time',
                  'shares', 'likes', 'comment_count','reactions', 'engagment']
    csv_data = []
    csv_data.insert(0, headerNodeFile)
    save_csv(outfile_nodes, csv_data, file_mode="a")

    headerEdgeFile = ['source', 'target']
    csv_data = []
    csv_data.insert(0, headerEdgeFile)
    save_csv(outfile_edges, csv_data, file_mode="a")

    for post in next_item['data']:
        addPostsAndCommentsToCSV(post,outfile_nodes,outfile_edges)

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges)

def buildReactionsCSVs(client_id, client_secret, site_id, since, until, outfile_nodes, outfile_edges, version="2.10"):
    fb_token = getAccessToken(client_id, client_secret)
    field_list = 'id,message,created_time,link,name,type,reactions'
    data_url = 'https://graph.facebook.com/v' + version + '/' + site_id + '/posts?fields=' + field_list + '&limit=100&since='+str(since)+'&until'+str(until)+'&'+fb_token
    next_item = url_retry(data_url)

    reaction_types = ['LIKE','LOVE','HAHA','WOW','SAD','ANGER']
    for reaction in reaction_types:
        headerNodeFile = ['node_id', 'type', 'reaction_type', 'link', 'name', 'message', 'created_time']
        csv_data = []
        csv_data.insert(0, headerNodeFile)
        save_csv(outfile_nodes + '_' + reaction + '.csv', csv_data, file_mode="a")

        headerEdgeFile = ['source', 'target']
        csv_data = []
        csv_data.insert(0, headerEdgeFile)
        save_csv(outfile_edges + '_' + reaction + '.csv', csv_data, file_mode="a")

    #headerNodeFile = ['node_id', 'type', 'reaction_type', 'link', 'name','message', 'created_time']
    #csv_data = []
    #csv_data.insert(0, headerNodeFile)
    #save_csv(outfile_nodes, csv_data, file_mode="a")
#
    #headerEdgeFile = ['source', 'target']
    #csv_data = []
    #csv_data.insert(0, headerEdgeFile)
    #save_csv(outfile_edges, csv_data, file_mode="a")

    for post in next_item['data']:
        addPostsAndReactionsToCSV(post, outfile_nodes, outfile_edges)

    while 'paging' in next_item and 'next' in next_item['paging']:
        next_item = url_retry(next_item['paging']['next'])
        for post in next_item['data']:
            addPostsAndCommentsToCSV(post, outfile_nodes, outfile_edges)

def generateCSV(mediaName, sinceDate, untilDate):
    # MediaTic's App's Info

    AppID = '264737207353432'
    AppSecret = '460c5a58dd6ddd6997b2645b1ad37cdd'

	# Dictionary of the Medios
    dicMedios = {'Nacion': '115872105050',
                 'CRHoy': '265769886798719',
                 'Financiero': '47921680333',
                 'Semanario': '119189668150973',
                 'Tico Times': '124823954224180',
                 'Extra': '109396282421232',
                 'Prensa Libre': '228302277255192',
                 'Telenoticias': '116842558326954',
                 'Repretel': '100237323349361',
                 'Monumental': '111416968911423'}

    date  = datetime.datetime.strptime(sinceDate, "%Y-%m-%d %H:%M:%S")
    since = calendar.timegm(date.utctimetuple())
    sinceFormated = str(datetime.datetime.strptime(sinceDate, "%Y-%m-%d %H:%M:%S").date())
    untilFormated = str(datetime.datetime.strptime(untilDate, "%Y-%m-%d %H:%M:%S").date())
    date  =  datetime.datetime.strptime(untilDate, "%Y-%m-%d %H:%M:%S")
    until = calendar.timegm(date.utctimetuple())

    buildPostCSVs(AppID, AppSecret, dicMedios[mediaName], since, until,
				  'nodes_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv',
				  'edges_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv',
				  version="2.10")

    buildCommentsCSVs(AppID, AppSecret, dicMedios[mediaName], since, until,
					  'nodes_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv',
					  'edges_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv',
					  version="2.10")

    zipedData = zipfile.ZipFile('uploads/'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.zip', mode='w')
    fileN = mediaName+'_'+sinceFormated+'-'+untilFormated+'.zip'
    try:
        zipedData.write('nodes_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        zipedData.write('edges_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        zipedData.write('nodes_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        zipedData.write('edges_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
    finally:
        zipedData.close()
        os.remove('nodes_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        os.remove('edges_posts_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        os.remove('nodes_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
        os.remove('edges_comments_'+mediaName+'_'+sinceFormated+'-'+untilFormated+'.csv')
    return fileN
