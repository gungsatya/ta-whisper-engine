import model

base_url = 'https://chatbot.citizenlab.web.id/berita/'
queryRunner = model.QueryRunner()
msgOutMod = model.MessageOut()
content = ''
news_queue = queryRunner.runGetResults('SELECT `news_id` FROM `news_broadcast` WHERE `is_broadcasted`=0 GROUP BY `news_id`')
for data in news_queue:

    news    = queryRunner.runGetResult('SELECT `title`, `news_url` FROM `news` WHERE `id`=%i'%(data['news_id']))
    users   = queryRunner.runGetResults('SELECT news_broadcast.id AS news_broadcast_id, news_broadcast.chat_id FROM news_broadcast '
                                        'JOIN pattern_control ON news_broadcast.chat_id = pattern_control.chat_id '
                                        'WHERE is_broadcasted=0 AND pattern_control.current_processed = "idle" AND '
                                        'news_broadcast.news_id=%i'%(data['news_id']))
    if(news is not None):
        content = ('<b>[BERITA TERBARU]\n'+
                   news['title']+"</b>\n"+
                   "\n"+
                   base_url+news['news_url'])
    for user in users:
        if (news is not None):
            msgOutMod.insert(user['chat_id'],'text', content)
        queryRunner.run('UPDATE news_broadcast SET is_broadcasted = 1 WHERE id=%i'%(user['news_broadcast_id']))

