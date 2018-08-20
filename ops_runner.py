import json
import model
import os
import time


def process(message_in_id):
    if(message_in_id is not None):
        print("Process message_in_id : %s" % (message_in_id))

        msgInMod = model.MessageIn()
        msgOutMod = model.MessageOut()
        statusPanelMod = model.PatternControl()
        opsMod = model.Operation()
        errorMsgMod = model.ErrorMessage()
        patternMod = model.Pattern()
        patternParamMod = model.PatternParam()
        paramInMod = model.ParamIn()
        paramOutMod = model.ParamOut()
        answerOptionMod = model.AnswerOption()
        commandMod = model.Command()
        queryRunner = model.QueryRunner()

        msg = msgInMod.getById(message_in_id)

        if (msg['flag'] == 'just_arrived'):

            msgInMod.updateFlag(msg['id'], 'read')
            pattern_control = statusPanelMod.getPathControlByChatId(msg['chat_id'])
            if (pattern_control is None):
                statusPanelMod.insert(msg['chat_id'])
                queryRunner.run('INSERT INTO news_broadcast(`chat_id`,`news_id`) VALUES(%i, 3)'%(msg['chat_id']))
                pattern_control = statusPanelMod.getPathControlByChatId(msg['chat_id'])
            if (pattern_control['current_processed'] == 'idle' or pattern_control['current_processed'] == 'operation'):

                statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'operation')
                if (pattern_control['current_ops_id'] is None or
                        pattern_control['current_param_in_id'] is None or
                        pattern_control['current_pattern_id'] is None):
                    syntax = str(msg['content']).replace(" ", "")
                    ops = opsMod.getBySyntax(syntax)
                    if (ops is None):
                        errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 1)
                        statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')
                        msgInMod.updateFlag(msg['id'], 'processed')
                        model.MessageInQueueOps().deleteMessage(message_in_id)
                        return 1
                    else:
                        pattern_id = patternMod.insert(msg['chat_id'], ops['id'])
                        pattern_param_id = patternParamMod.insert(pattern_id, msg['id'], 'chat_id', msg['chat_id'])
                        statusPanelMod.changeOpsId(msg['chat_id'], ops['id'])
                        statusPanelMod.changePatternId(msg['chat_id'], pattern_id)
                        statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)
                        pattern_control['current_ops_id'] = ops['id']
                        pattern_control['current_pattern_id'] = pattern_id
                else:

                    if(msg['content']=='/back'):
                        params = patternParamMod.getAllParamValue(pattern_control['current_pattern_id'])
                        patternParamMod.deleteByParam(params[0]['pattern_id'] , params[0]['param'])
                    else:
                        param = paramInMod.getById(pattern_control['current_param_in_id'])
                        if (param['question_type'] == 'open'):
                            if (param['is_long_answer']):
                                if (str(msg['content']).replace(" ", "") != '###'):
                                    if (param['answer_type'] != msg['content_type']):
                                        errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 2)
                                        model.MessageInQueueOps().deleteMessage(message_in_id)
                                        return 1
                                    msgOutMod.insert(msg['chat_id'], 'text', 'Ada lagi ?\n\nJika sudah, kirimkan pesan <pre>###</pre> untuk melanjutkan proses.')
                                    if (pattern_control['current_pattern_param_id'] is None):
                                        pattern_param_id = patternParamMod.insert(pattern_control['current_pattern_id'],
                                                                                  msg['id'], param['param_in'], msg['content'])
                                        statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)
                                        model.MessageInQueueOps().deleteMessage(message_in_id)
                                        msgInMod.updateFlag(msg['id'], 'processed')
                                        return 1
                                    else:
                                        patternParamMod.addValueParam(pattern_control['current_pattern_param_id'],
                                                                      msg['content'])
                                        model.MessageInQueueOps().deleteMessage(message_in_id)
                                        msgInMod.updateFlag(msg['id'], 'processed')
                                        return 1
                                else:
                                    if (pattern_control['current_pattern_param_id'] is None):
                                        msgOutMod.insert(msg['chat_id'],'text','Isi konten terlebih dahulu sebelum mengakhiri pertanyaan ini.')
                                        model.MessageInQueueOps().deleteMessage(message_in_id)
                                        msgInMod.updateFlag(msg['id'], 'processed')
                                        return 1
                            else:
                                if (param['answer_type'] != msg['content_type']):
                                    errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 2)
                                    model.MessageInQueueOps().deleteMessage(message_in_id)
                                    return 1

                                pattern_param_id = patternParamMod.insert(pattern_control['current_pattern_id'], msg['id'],
                                                                          param['param_in'], msg['content'])
                                statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)
                        else:
                            if (param['answer_type'] == 'text'):
                                is_right_ans = False
                                options = answerOptionMod.getByParamInId(pattern_control["current_param_in_id"])
                                for opt in options:
                                    if (opt["answer"] == str(msg['content'])):
                                        pattern_param_id = patternParamMod.insert(pattern_control['current_pattern_id'],
                                                                                  msg['id'],
                                                                                  param['param_in'], opt['val'])
                                        statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)
                                        is_right_ans = True
                                        break

                                if (not is_right_ans):
                                    errorMsgMod.sendToMsgOut(msg['chat_id'], msg['message_id'], 3, er_is_replymarkup=1,
                                                             er_reply_markup=options)
                                    model.MessageInQueueOps().deleteMessage(message_in_id)
                                    msgInMod.updateFlag(msg['id'], 'processed')
                                    return 1
                            else:
                                pattern_param_id = patternParamMod.insert(pattern_control['current_pattern_id'], msg['id'],
                                                                  param['param_in'], msg['content'])
                                statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)


                param_in = paramInMod.getByOpsId(pattern_control['current_ops_id'])
                finish = True

                temp = 1

                for param in param_in:
                    if (patternParamMod.countParam(pattern_control['current_pattern_id'], param['param_in']) != 1):
                        patternParamMod.deleteByParam(pattern_control['current_pattern_id'], param['param_in'])
                        finish = False
                        if (not param['is_restricted']):
                            if (param['question_type'] == 'close'):
                                answerOption = answerOptionMod.getByParamInId(param['id'])
                                content = ("<b>[Pertanyaan %i dari %i pertanyaan]</b>\n\n" % (temp, len(param_in)) +
                                           param['question'] + "\n\n" +
                                           "<i>Pilih jawaban yang ada pada tombol markup</i>\n\n"+
                                           "/back untuk kembali ke pertanyaan sebelumnya\n" +
                                           "/clear untuk keluar dari operasi")

                                msgOutMod.insert(msg['chat_id'], 'text', content, is_replymarkup=1,
                                                 reply_markup=answerOption)
                            else:
                                content = ("<b>[Pertanyaan %i dari %i pertanyaan]</b>\n\n" % (temp, len(param_in)) +
                                           param['question'] + "\n\n" +
                                           "<i>Silahkan kirim jawaban Anda</i>\n"+
                                           "/back untuk kembali ke pertanyaan sebelumnya\n\n" +
                                           "/clear untuk keluar dari operasi")
                                msgOutMod.insert(msg['chat_id'], 'text', content)
                            statusPanelMod.changeParamInId(msg['chat_id'], param['id'])
                            statusPanelMod.changePatternParamId(msg['chat_id'], "NULL")
                            break
                        else:
                            allowed = True
                            require_params = json.loads(param['require_param'])
                            for require in require_params:
                                value = patternParamMod.getParamValue(pattern_control['current_pattern_id'],
                                                                      require['param'])
                                if (require['value'] != value):
                                    allowed = False
                            if (allowed):
                                if (param['question_type'] == 'close'):
                                    answerOption = answerOptionMod.getByParamInId(param['id'])
                                    
                                    content = ("<b>[Pertanyaan %i dari %i pertanyaan]</b>\n\n" % (temp, len(param_in)) +
                                               param['question'] + "\n\n" +
                                               "<i>Pilih jawaban yang ada pada tombol markup</i>\n"+
                                               "/back untuk kembali ke pertanyaan sebelumnya\n\n" +
                                               "/clear untuk keluar dari operasi")

                                    msgOutMod.insert(msg['chat_id'], 'text', content, is_replymarkup=1,
                                                     reply_markup=answerOption)
                                else:
                                    content = ("<b>[Pertanyaan %i dari %i pertanyaan]</b>\n\n" % (temp, len(param_in)) +
                                               param['question'] + "\n\n" +
                                               "<i>Silahkan kirim jawaban Anda</i>\n\n"+
                                               "/back untuk kembali ke pertanyaan sebelumnya\n" +
                                               "/clear untuk keluar dari operasi")
                                    msgOutMod.insert(msg['chat_id'], 'text', content)
                                statusPanelMod.changeParamInId(msg['chat_id'], param['id'])
                                statusPanelMod.changePatternParamId(msg['chat_id'], "NULL")
                                break
                            else:
                                if (param['is_buffer_value']):
                                    pattern_param_id = patternParamMod.insert(pattern_control['current_pattern_id'],
                                                                              msg['id'],
                                                                              param['param_in'],
                                                                              param['default_value'])
                                    statusPanelMod.changePatternParamId(msg['chat_id'], pattern_param_id)
                                finish = True
                                continue
                    temp +=1

                if (finish):

                    ops = opsMod.getById(pattern_control['current_ops_id'])

                    if (ops['is_sql_command']):
                        params = patternParamMod.getAllParamValue(pattern_control['current_pattern_id'])
                        query = str(ops['in_sql'])
                        for param in params:
                            query = query.replace("<<" + param['param'] + ">>", param["param_value"])
                        cmd_id = commandMod.insert(ops["id"], msg['chat_id'], query)

                        if (not ops["is_read_command"]):

                            queryRunner.run(query)

                            paramOut = paramOutMod.getByOpsId(ops['id'])
                            for response in paramOut:
                                msgOutMod.insert(msg["chat_id"], response["response_type"], response["response"])
                        else:

                            fields = queryRunner.runGetResults(query)
                            paramOut = paramOutMod.getByOpsId(ops['id'])
                            for field in fields:
                                for response in paramOut:
                                    content = str(response["response"])
                                    for key, value in field.items():
                                        content = content.replace("<<" + key + ">>", value)
                                    msgOutMod.insert(msg["chat_id"], response["response_type"], content)
                    else:
                        cmd_id = commandMod.insert(ops["id"], msg['chat_id'], "")

                        paramOut = paramOutMod.getByOpsId(ops['id'])
                        for response in paramOut:
                            msgOutMod.insert(msg["chat_id"], "text", response["response"])

                    commandMod.changeFlag(cmd_id, "processed")
                    patternMod.changeFlag(pattern_control['current_pattern_id'], "finish")
                    statusPanelMod.changeOpsId(msg['chat_id'], "NULL")
                    statusPanelMod.changeParamInId(msg['chat_id'], "NULL")
                    statusPanelMod.changePatternId(msg['chat_id'], "NULL")
                    statusPanelMod.changePatternParamId(msg['chat_id'], "NULL")
                    statusPanelMod.changeCurrentProcessed(msg['chat_id'], 'idle')

            msgInMod.updateFlag(msg['id'], 'processed')

        model.MessageInQueueOps().deleteMessage(message_in_id)

    else:
        model.MessageInQueueOps().deleteNullMessage()


def main():
    start_time = time.time()
    try:
        model.ConRecord().insert(int(os.getpid()), "ops_runner")
    except:
        print("Can't create session")
    else:
        while round(time.time() - start_time) < 360:
            try:
                queues = model.MessageInQueueOps().getMessage()
                if queues is not None:
                    for queue in queues:
                        process(queue[0])

            except Exception as e:
                print("Something problem : %s" % (e))
            time.sleep(0.2)
        model.ConRecord().updateOff(int(os.getpid()))
        os.system(
            'nohup /home/citizenl/virtualenv/engine__python/3.5/bin/python /home/citizenl/engine_python_new/ops_runner.py >> ops_log.log &')


if __name__ == '__main__':
    main()
