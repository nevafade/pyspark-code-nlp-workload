from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,length,size
from pyspark import Row
import argparse

window = 4096
stride = 2048

def sample(input):
  contract_end = len(input)-1
  index = 0
  seqListLength = contract_end/stride
  listLength = 0
  if (seqListLength < 1) & (seqListLength > 0) :
    listLength = 1
  else:
    listLength = int(seqListLength)
  seqList = [Row(start=x*stride,end=x*stride+window) if x*stride+window<contract_end else {'start':x*stride,'end':contract_end} for x in range(0,int(listLength))]
  return {'contract':input,'samples':seqList}

def getAnsEnd(row):
    q = row.questions
    ansSet = q.answers
    newAns = []
    for ans in ansSet:
      answer_end=ans.answer_start+len(ans.text)
      newAns.append(Row(end=answer_end,start=ans.answer_start,text=ans.text))
    newquestion = Row(id=q.id,is_impossible=q.is_impossible,question=q.question,answers=newAns)
    return Row(questionID=row.questionID,questions=newquestion,samples=row.samples,contract=row.contract)

def markSample(row):
  question = row.questions
  answers = question.answers
  mark=False
  for ans in answers:
    a = ans.start
    b = row.sample.start
    if(ans.start > row.sample.start and ans.end < row.sample.end):
      mark=True
      break;
      #return Row(contract=row.contract,samples=row.samples,categories=row.categories,sample=Row(start=row.ans.start,end=row.ans.end,positive=True,index=int(row.sample.start/stride)),ans=row.ans)
    if (ans.start < row.sample.start and ans.end < row.sample.end and ans.end > row.sample.start):
      mark=True
      break;
      #return Row(contract=row.contract,samples=row.samples,categories=row.categories,sample=Row(start=row.sample.start,end=row.ans.end,positive=True,index=int(row.sample.start/stride)),ans=row.ans)
    if (ans.start > row.sample.start and ans.end > row.sample.end and ans.start < row.sample.end):
      mark=True
      break;
      #return Row(contract=row.contract,samples=row.samples,categories=row.categories,sample=Row(start=row.ans.start,end=row.sample.end,positive=True,index=int(row.sample.start/stride)),ans=row.ans)
    if (ans.start < row.sample.start and ans.end > row.sample.end):
      mark=True
      break;
      #return Row(contract=row.contract,samples=row.samples,categories=row.categories,sample=Row(start=row.sample.start,end=row.sample.end,positive=True,index=int(row.sample.start/stride)),ans=row.ans)
  return Row(questionID=row.questionID,sample=Row(start=row.sample.start,end=row.sample.end,positive=mark))

def possitiveCount(question):
  count = len([x for x in question.sampleSetForQuestion if x[2]])
  return Row(categoryName=question.categoryName,contract=question.contract,questions=question.questions,sampleSetForQuestion=question.sampleSetForQuestion,possitiveCountForQuestion=count)

def extractSelectedSet(contracts):
  no_of_impossible_samples_to_be_selected = 0
  return_contracts = []
  for row in contracts:
    if not row[2].is_impossible:
      no_of_impossible_samples_to_be_selected += row[4]
      selectedSet = [x for x in row[3] if x[2]]
      negitiveSet = [x for x in row[3] if not x[2]][0:len(selectedSet)]
      selectedSet.extend(negitiveSet)
      row.append(selectedSet)
      row.append(0)
  for row in contracts:
    if row[2].is_impossible:
      row.append([])
      if len([x for x in contracts if not x[2].is_impossible]):
        row.append(round(no_of_impossible_samples_to_be_selected/len([x for x in contracts if not x[2].is_impossible])))
      else:
        row.append(len(row[3]))
    return_contracts.append(Row(categoryName=row[0],contract=row[1],question=row[2],sampleSetForQuestion=row[3],possitiveCountforQuestion=row[4],impossibleCountForQuestion=row[6],selectedSetForQuestion=row[5]))
  return return_contracts

def extractSelectedSamples(questions):
  selectedSet= []
  for question in questions:
    if len(question.selectedSetForQuestion):
      selectedSet.extend(question.selectedSetForQuestion)
  selectedDict_n = {x[0]:x for x in selectedSet if not x[2]}
  selectedDict_p = {x[0]:x for x in selectedSet if x[2]}
  selectedDict = {**selectedDict_n,**selectedDict_p}
  return (questions,selectedDict)

def selectImpossibleSamples(x):
  selectedDict = x[1]
  questions = x[0]
  for q in questions:
    if q.question.is_impossible:
      selected = q.selectedSetForQuestion
      count = q.impossibleCountForQuestion
      samples = q.sampleSetForQuestion
      for s in samples:
        if s[0] not in selectedDict.keys():
          selected.append(s)
      if len(selected) > q.impossibleCountForQuestion:
        selected = selected[0:q.impossibleCountForQuestion]
      else:
        positiveDict = {k:v for k,v in selectedDict.items() if v[2]}
        selected = []
        if s[0] not in positiveDict.keys():
          selected.append(s)
        if len(selected) > q.impossibleCountForQuestion:
          selected = selected[0:q.impossibleCountForQuestion]
      q = Row(question=q.question,selectedSetForQuestion=selected)
  return questions

def calculateStartEnd(sample):
  answers = sample[2]
  s = {}
  if not len(answers):
    s['start'] = 0
    s['end'] = 0
  for ans in answers:
    ans_start = ans.start
    ans_end = ans.end
    if sample[3][2]:
      seq_start = sample[3].start
      seq_end = sample[3].end
      if ans_start < seq_start and ans_end > seq_end:
        s['start'] = 0
        s['end'] = window
        s['condition'] = 'condition 1'
      elif ans_start < seq_start and ans_end > seq_start and ans_end < seq_end:
        s['start'] = 0
        s['end'] = ans_end - seq_start
        s['condition'] = 'condition 2'
      elif ans_start > seq_start and ans_start < seq_end and ans_end > seq_end:
        s['start'] = ans_start - seq_start
        s['end'] = window
        s['condition'] = 'condition 3'
      elif ans_start > seq_start and ans_start < seq_end and ans_end < seq_end:
        s['start'] = ans_start - seq_start
        s['end'] = ans_end - seq_start
        s['condition'] = 'condition 4'
      else:
        s['start'] = 0
        s['end'] = 0
        s['condition'] = 'condition 5'
    else:
      s['start'] = 0
      s['end'] = 0
      s['condition'] = 'condition 6'
  return (sample[0],sample[1],s['start'],s['end'])



if __name__ == "__main__":
    spark = SparkSession.builder.appName("Assignment 2").getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="the output path",
                        default='sample_json')
    parser.add_argument("--input", help="dataset json",default='test.json')
    args = parser.parse_args()
    output_path = args.output
    input_path = args.input
    json = spark.read.json(input_path)
    jsonData = json.select((explode("data").alias('set')))
    jsontitle = jsonData.withColumn('title',jsonData.set.title)
    jsonParagraphs = jsontitle.withColumn('paragraphs',jsonData.set.paragraphs)
    jsonSD = jsonParagraphs.withColumn('para',explode('paragraphs'))
    jsonSD = jsonSD.drop('set')
    jsonSD = jsonSD.drop('paragraphs')
    jsoncontext = jsonSD.withColumn('contract',jsonSD.para.context).withColumn('QAS',jsonSD.para.qas)
    df_contract = jsoncontext.select('contract')
    df_samples = df_contract.rdd.map(lambda x:x.contract).map(sample).toDF()
    df_qas = jsoncontext.drop('title','para')
    df_qas = df_qas.withColumnRenamed('contract','context')
    df_join = df_samples.join(df_qas,df_qas.context == df_samples.contract)
    df_for_intel = df_join.select('contract','samples','QAS').withColumn('questions',explode('QAS'))
    df_for_intel = df_for_intel.withColumn('questionID',df_for_intel.questions.id)
    df_for_intel = df_for_intel.drop('QAS')
    df_for_intel = df_for_intel.rdd.map(getAnsEnd).toDF()
    rdd_mark = df_for_intel.select('questionID','samples','questions').withColumn('sample',explode('samples')).rdd.map(markSample)
    rdd_sample_set_for_question = rdd_mark.map(lambda x:(x.questionID,x.sample)).groupByKey().mapValues(lambda x:list(x)).map(lambda x:(x[0],Row(categoryName=x[0][x[0].rfind('_')+1:],questionID=x[0],sampleSetForQuestion=x[1])))
    df_for_intel.drop('samples')
    rdd_intel = df_for_intel.rdd.map(lambda x:(x.questionID,x))
    rdd_join = rdd_intel.join(rdd_sample_set_for_question).mapValues(lambda x:Row(categoryName=x[1].categoryName,contract=x[0].contract,questions=x[0].questions,sampleSetForQuestion=x[1].sampleSetForQuestion)).map(lambda x:x[1]).map(possitiveCount)
    df_group_by_category_name = rdd_join.map(lambda x:(x.categoryName,[x.categoryName,x.contract,x.questions,x.sampleSetForQuestion,x.possitiveCountForQuestion])).groupByKey().map(lambda x:(x[0],list(x[1])))
    rdd_impossible_set_for_category = df_group_by_category_name.flatMapValues(extractSelectedSet)
    rdd_group_by_contract = rdd_impossible_set_for_category.map(lambda x:(x[1].contract,x[1])).groupByKey().map(lambda x:(x[0],list(x[1])))
    rdd_questions_with_possitive_samples = rdd_group_by_contract.mapValues(extractSelectedSamples)
    rdd_selected_samples = rdd_questions_with_possitive_samples.flatMapValues(selectImpossibleSamples).map(lambda x:((x[0],x[1].question),x[1].selectedSetForQuestion)).flatMapValues(lambda x:x)
    rdd_samples_with_source = rdd_selected_samples.map(lambda x:(x[0][0],x[0][1],x[1])).map(lambda x:(x[0][x[2][0]:x[2][1]],x[1].question,x[1].answers,x[2])).map(calculateStartEnd)
    df_output = rdd_samples_with_source.toDF()
    df_output.write.json(output_path)
