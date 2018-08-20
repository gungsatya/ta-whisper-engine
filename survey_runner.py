import json
import model
import os
import time


def process(message_in_id):
    if (message_in_id is not None):
        print("Process message_in_id : %s" % (message_in_id))

        msgInMod = model.MessageIn()
        msgOutMod = model.MessageOut()
        statusPanelMod = model.PatternControl()
        errorMsgMod = model.ErrorMessage()
        surveyMod = model.Survey()

        msg = msgInMod.getById(message_in_id)

        if (msg['flag'] == 'just_arrived'):

            msgInMod.updateFlag(msg['id'], 'read')
            pattern_control = statusPanelMod.getPathControlByChatId(msg['chat_id'])

            if(pattern_control['current_processed'] == 'tmp_survey'):

                survey = surveyMod.getById(pattern_control['temp_survey_id'])
                if (survey is None):
                    statusPanelMod.changeCurrentSurveyId(msg['chat_id'], 'NULL')
                    statusPanelMod.changeCurrentSurveyQuestionId(msg['chat_id'], 'NULL')
                    statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')
                    msgOutMod.insert(msg['chat_id'], 'text', 'Survei saat ini tidak ada atau telah selesai')
                    return 1

                if(str(msg['content']).lower() == 'ya'):
                    statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'survey')
                    statusPanelMod.changeCurrentSurveyId(msg['chat_id'], pattern_control['temp_survey_id'])
                    statusPanelMod.changeTempSurveyId(msg['chat_id'],"NULL")
                    pattern_control = statusPanelMod.getPathControlByChatId(msg['chat_id'])

                elif(str(msg['content']).lower() == 'tidak'):
                    statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')
                    statusPanelMod.changeTempSurveyId(msg['chat_id'], "NULL")
                    msgOutMod.insert(msg['chat_id'], 'text', 'Survei dibatalkan')
                    msgInMod.updateFlag(msg['id'], 'processed')
                    model.MessageInQueueSurvey().deleteMessage(message_in_id)
                    return 1
                else:
                    errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 3)
                    msgInMod.updateFlag(msg['id'], 'processed')
                    model.MessageInQueueSurvey().deleteMessage(message_in_id)
                    return 1

            elif(pattern_control['current_processed'] == 'survey'):

                survey = surveyMod.getById(pattern_control['current_survey_id'])
                if (survey is None):
                    statusPanelMod.changeCurrentSurveyId(msg['chat_id'], 'NULL')
                    statusPanelMod.changeCurrentSurveyQuestionId(msg['chat_id'], 'NULL')
                    statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')
                    msgOutMod.insert(msg['chat_id'], 'text', 'Survei saat ini tidak ada atau telah selesai')
                    return 1

                question = surveyMod.getQuestionById(pattern_control['current_survey_question_id'])

                if(question['is_closed']):
                    answers = str(question['answers']).lower().replace(" ","").split(",")
                    if(str(msg['content']).lower().replace(" ","") in answers):
                        surveyMod.insertRespond(pattern_control['current_survey_id'],
                                                pattern_control['current_survey_question_id'],
                                                msg['chat_id'], msg['content'])
                    else:
                        errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 3)
                        msgInMod.updateFlag(msg['id'], 'processed')
                        model.MessageInQueueSurvey().deleteMessage(message_in_id)
                        return 1
                else:
                    surveyMod.insertRespond(pattern_control['current_survey_id'],
                                            pattern_control['current_survey_question_id'],
                                            msg['chat_id'], msg['content'])
                
            finish = True
            questions = surveyMod.getQuestionBySurveyId(pattern_control['current_survey_id'])
            temp = 1
            for question in questions:
                respond = surveyMod.countRespond(question['id'], msg['chat_id'])
                if(respond is None or respond<1):
                    finish = False
                    statusPanelMod.changeCurrentSurveyQuestionId(msg['chat_id'], question['id'])
                    if(question['is_closed']):
                        answerOption = []
                        answers = str(question['answers']).split(",")
                        for answer in answers:
                            answerOption.append({'markup_type':'text', 'answer':answer})

                        content = ("<b>Pertanyaan %i dari %i pertanyaan</b> \n\n"%(temp, len(questions))+
                                   question['question']+"\n\n"+
                                   "<i>Pilih jawaban yang ada pada tombol markup</i>\n"+
                                   "/clear untuk keluar dari operasi")
                        msgOutMod.insert(msg['chat_id'], 'text', content, is_replymarkup=1, reply_markup=answerOption)
                    else:
                        content = ("<b>Pertanyaan %i dari %i pertanyaan</b> \n\n" % (temp, len(questions)) +
                                   question['question'] + "\n\n" +
                                   "<i>Silahkan kirim jawaban Anda</i>\n"+
                                   "/clear untuk keluar dari operasi")
                        msgOutMod.insert(msg['chat_id'], 'text', content)
                    break

                temp+=1

            if(finish):
                statusPanelMod.changeCurrentSurveyId(msg['chat_id'],'NULL')
                statusPanelMod.changeCurrentSurveyQuestionId(msg['chat_id'], 'NULL')
                statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')
                msgOutMod.insert(msg['chat_id'], 'text', 'Terimakasih telah mengikuti survei.')
                msgOutMod.insert(msg['chat_id'], 'text', 'Hasil survei dapat dilihat pada <a href="https://chatbot.citizenlab.web.id/survei/%i">link ini</a>.' % (pattern_control['current_survey_id']))

            msgInMod.updateFlag(msg['id'], 'processed')

        model.MessageInQueueSurvey().deleteMessage(message_in_id)
    else:
        model.MessageInQueueSurvey().deleteNullMessage()


def main():
    start_time = time.time()
    try:
        model.ConRecord().insert(int(os.getpid()), "survey_runner")
    except:
        print("Can't create session")
    else:
        while round(time.time() - start_time) < 360:
            try:
                queues = model.MessageInQueueSurvey().getMessage()
                if queues is not None:
                    for queue in queues:
                        process(queue[0])
            except Exception as e:
                print("Something problem : %s" % (e))
            time.sleep(0.3)
        model.ConRecord().updateOff(int(os.getpid()))
        os.system('nohup /home/citizenl/virtualenv/engine__python/3.5/bin/python /home/citizenl/engine_python_new/survey_runner.py >> survey.log &')


if __name__ == '__main__':
    main()
