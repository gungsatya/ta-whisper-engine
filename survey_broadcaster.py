import model

queryRunner     = model.QueryRunner()
msgOutMod       = model.MessageOut()
content = ''
answerOption = [
        {'markup_type':'text', 'answer':'Ya'},
        {'markup_type':'text', 'answer':'Tidak'}
]

survey_queue = queryRunner.runGetResults('SELECT `survey_id` FROM `survey_broadcast` WHERE `is_broadcasted`=0 GROUP BY `survey_id`')
for data in survey_queue:

    survey  = queryRunner.runGetResult('SELECT `title`, `explanation` FROM `survey` WHERE `due_at`>NOW() AND `id`=%i'%(data['survey_id']))
    users   = queryRunner.runGetResults('SELECT survey_broadcast.id AS survey_broadcast_id, survey_broadcast.chat_id FROM survey_broadcast '
                                        'JOIN pattern_control ON survey_broadcast.chat_id = pattern_control.chat_id '
                                        'WHERE is_broadcasted=0 AND pattern_control.current_processed = "idle" AND '
                                        'survey_broadcast.survey_id=%i'%(data['survey_id']))
    if(survey is not None):
        content = ("<b>[SURVEI]\n" +
                   str(survey['title']).upper() + "</b>\n\n" +
                   survey['explanation']+ "\n\n" +
                   "<i>Apakah Anda bersedia menjawab survei ini ?</i>")

    for user in users:
        if (survey is not None):
            msgOutMod.insert(user['chat_id'],'text', content, is_replymarkup=1, reply_markup=answerOption)
            queryRunner.run(
                'UPDATE pattern_control SET current_processed = "tmp_survey", temp_survey_id=%i WHERE chat_id=%i'
                % (data['survey_id'], user['chat_id']))
        queryRunner.run('UPDATE survey_broadcast SET is_broadcasted = 1 WHERE id=%i'%(user['survey_broadcast_id']))

