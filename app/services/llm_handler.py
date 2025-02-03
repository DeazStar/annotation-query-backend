import os
from dotenv import load_dotenv
from app.services.llm_models import OpenAIModel, GeminiModel
from app.services.graph_handler import Graph_Summarizer
load_dotenv()

class LLMHandler:
    def __init__(self):
        model_type = os.getenv('LLM_MODEL')

        if model_type == 'openai':
            openai_api_key = os.getenv('OPENAI_API_KEY')
            if not openai_api_key:
                raise ValueError("OpenAI API key not found")
            self.model = OpenAIModel(openai_api_key)
        elif model_type == 'gemini':
            gemini_api_key = os.getenv('GEMINI_API_KEY')
            if not gemini_api_key:
                raise ValueError("Gemini API key not found")
            self.model = GeminiModel(gemini_api_key)
        else:
            raise ValueError("Invalid model type in configuration")

    def generate_title(self, query):
        prompt = f'''From this query generate approperiate title. Only give the title sentence don't add any prefix.
                     Query: {query}'''
        title = self.model.generate(prompt)
        return title

    def generate_summary(self, graph,data_value=None, user_query=None,graph_id=None, summary=None):
        summarizer = Graph_Summarizer(self.model)
        summary = summarizer.summary(graph, user_query, graph_id, summary,data_value)
        print("Direct summary ",summary )
         
        # if data_value:
        #     def extract_node(nodes):
        #         result=[]
        #         for node in nodes:
        #             result.append(f"Node '{node['node_id']} represents a {node[type]} with ID{node['id']}")
        #             return "".json(result)
        #     def extract_node(predicates):
        #         result=[]
        #         for predicate in predicates:
        #             result.append(f"{predicate['predicate_id']} is of type {predicate['type']},connecting"
        #                           f"node {predicate['source']} to node {predicate['target']}" )
        #             return " ".join(result)
                
        #     def extract_logic(logic,nodes_dict):
        #         summary=[]
        #         def parse_logic(logic):
        #             if "operator" in logic:
        #                 operator=logic['operator']
        #                 if operator=='NOT':
        #                     summary.append( f"Logical condition 'NOT' is applied to exclude node '{logic['nodes']['node_id']}' "
        #             f"({nodes_dict[logic['nodes']['node_id']]['type']}).")
        #                     if operator=='AND':
        #                         summary.append(f"Logical condition 'AND' connects:")
        #                     if operator=='OR':
        #                         summary.append(f"Logical condtion 'OR alternative if one from the two is found it is satisfied  '")
        #                 return "" .join(summary)
        #     nodes_summary=extract_node(data_value['requests']['nodes'])
        #     predicate_summary=extract_node(data_value['reqiests']['nodes'])
        #     logic_summary=extract_logic(data_value['requests']['logic'])
        # summarizer = Graph_Summarizer(self.model)
        # summary = summarizer.summary(graph, user_query, graph_id, summary,nodes_summary,predicate_summary,logic_summary)
        # print("summary ____________________________((((((((((((((((((((((((((((((((((((((((((Second summary))))))))))))))))))))))))))))))))))))))))))")
        # print("Summary_____________________________________________________________________________",summary)
        # print("summary ____________________________((((((((((((((((((((((((((((((((((((((((((summary))))))))))))))))))))))))))))))))))))))))))")
        return summary
