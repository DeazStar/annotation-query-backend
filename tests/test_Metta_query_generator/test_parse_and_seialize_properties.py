import pytest
from hyperon import E, S, V

def test_onenode_oneexpressionAtom_variableatom(runner):
    with pytest.raises(AttributeError):
        test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), V("ENSG00000101349")), S("PAK5")))]
        runner.parse_and_serialize_properties(test)

def test_onenode_oneexpressionAtom_tuple(runner):
    with pytest.raises(TypeError):
        test = (E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5"))))
        runner.parse_and_serialize_properties(test)

def test_oneexpressionAtom(runner):
    with pytest.raises(TypeError):
        test = E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")))
        runner.parse_and_serialize_properties(test)

def test_onesymbolicAtom(runner):
    with pytest.raises(TypeError):
        test = S("node")
        runner.parse_and_serialize_properties(test)

def test_onevariableAtom(runner):
    with pytest.raises(TypeError):
        test = V("node")
        runner.parse_and_serialize_properties(test)

def test_tuple(runner):
    output = runner.parse_and_serialize_properties(())
    assert output == [[],[]]
    assert output == [[],[]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_set(runner):
    output = runner.parse_and_serialize_properties(set())
    assert output == [[],[]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_onenode_oneexpressionAtom_set(runner):
    test = (E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5"))), )
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{'data': {'gene_name': 'PAK5', 'id': 'gene ENSG00000101349', 'type': 'gene'}}], []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_list(runner):
    output = runner.parse_and_serialize_properties([])
    assert output == [[],[]]
    assert output == [[],[]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_onenode_oneexpressionAtom_wronglength(runner):
    with pytest.raises(ValueError):
        test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349"))))]
        runner.parse_and_serialize_properties(test)
    

def test_onenode_oneexpressionAtom(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")))]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{'data': {'gene_name': 'PAK5', 'id': 'gene ENSG00000101349', 'type': 'gene'}}], []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']


    
def test_onenode_twoexpressionAtom(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000101349")), S("protein_coding"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{
        'data': {
            'gene_name': 'PAK5', 
            'id': 'gene ENSG00000101349', 
            'type': 'gene',
            'gene_type': 'protein_coding'
            }
        }], []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_onenode_twoexpressionAtom_twoid(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000203459")), S("protein_coding"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{
        'data': {
            'gene_name': 'PAK5', 
            'id': 'gene ENSG00000101349', 
            'type': 'gene',
            }
        },{
        'data': {
            'id': 'gene ENSG00000203459', 
            'type': 'gene',
            'gene_type': 'protein_coding'
            }
        }], []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_twonode_twoexpressionAtom(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000101349")), S("protein_coding"))
            ),
            E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000603349")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000603349")), S("protein_coding"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{
        'data': {
            'id': 'gene ENSG00000101349', 
            'gene_name': 'PAK5', 
            'type': 'gene',
            'gene_type': 'protein_coding'
            }},{ 
        'data': {
            'id': 'gene ENSG00000603349', 
            'gene_name': 'PAK5', 
            'type': 'gene',
            'gene_type': 'protein_coding'
        }}], 
        []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_twonode_twoexpressionAtom_fourid(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000202459")), S("protein_coding"))
            ),
            E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000303569")), S("PAK5")),
                E(S("node"), S("gene_type"), E(S("gene"), S("ENSG00000404679")), S("protein_coding"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[{
        'data': {
            'id': 'gene ENSG00000101349', 
            'gene_name': 'PAK5', 
            'type': 'gene',
            }},
        {'data': {
            'id': 'gene ENSG00000202459', 
            'type': 'gene',
            'gene_type': 'protein_coding'
        }},
        {'data': {
            'id': 'gene ENSG00000303569', 
            'gene_name': 'PAK5', 
            'type': 'gene',
        }},
        {'data': {
            'id': 'gene ENSG00000404679', 
            'type': 'gene',
            'gene_type': 'protein_coding'
        }}],
        []]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for i in range(len(output[0])):
        assert list(output[0][i].keys()) == ['data']

def test_oneedge_oneexpressionAtom(runner):
    test = [E(
                E(S("edge"), S("source"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("GENCODE"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[], [
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000101349', 
            'target': 'transcript ENST00000353224', 
            'source_data': 'GENCODE'
            }}
        ]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for j in range(2):
        for i in range(len(output[j])):
            assert list(output[j][i].keys()) == ['data']

def test_oneedge_twoexpressionAtom(runner):
    test = [E(
                E(S("edge"), S("source"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("GENCODE")),
                E(S("edge"), S("source_url"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("https://www.gencodegenes.org/human/"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[], [
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000101349', 
            'target': 'transcript ENST00000353224', 
            'source_data': 'GENCODE',
            'source_url': 'https://www.gencodegenes.org/human/'
            }}
        ]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for j in range(2):
        for i in range(len(output[j])):
            assert list(output[j][i].keys()) == ['data']

def test_oneedge_twoexpressionAtom_threeid(runner):
    test = [E(
                E(S("edge"), S("source"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("GENCODE")),
                E(S("edge"), S("source_url"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000203459")), E(S("transcript"), S("ENST00000353224"))), S("https://www.gencodegenes.org/human/"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[], [
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000101349', 
            'target': 'transcript ENST00000353224', 
            'source_data': 'GENCODE'
            }},
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000203459', 
            'target': 'transcript ENST00000353224', 
            'source_url': 'https://www.gencodegenes.org/human/'
            }}
        ]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for j in range(2):
        for i in range(len(output[j])):
            assert list(output[j][i].keys()) == ['data']

def test_twoedge_twoexpressionAtom(runner):
    test = [E(
                E(S("edge"), S("source"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("GENCODE"))),
            E(
                E(S("edge"), S("source_url"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("https://www.gencodegenes.org/human/"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[], [
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000101349', 
            'target': 'transcript ENST00000353224', 
            'source_data': 'GENCODE',
            'source_url': 'https://www.gencodegenes.org/human/'
            }}
        ]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for j in range(2):
        for i in range(len(output[j])):
            assert list(output[j][i].keys()) == ['data']

def test_onenode_oneedge_twoexpressionAtom(runner):
    test = [E(
                E(S("node"), S("gene_name"), E(S("gene"), S("ENSG00000101349")), S("PAK5"))),
            E(
                E(S("edge"), S("source_url"), E(S("transcribed_to"), E(S("gene"), S("ENSG00000101349")), E(S("transcript"), S("ENST00000353224"))), S("https://www.gencodegenes.org/human/"))
            )]
    output = runner.parse_and_serialize_properties(test)
    assert output == [[
        {'data': {
            'gene_name': 'PAK5',
            'id': 'gene ENSG00000101349',
            'type': 'gene'}}], [
        {'data': {
            'label': 'transcribed_to', 
            'source': 'gene ENSG00000101349', 
            'target': 'transcript ENST00000353224', 
            'source_url': 'https://www.gencodegenes.org/human/'
            }}
        ]]
    assert type(output) is list
    assert type(output[0]) is list
    assert type(output[1]) is list
    assert len(output) == 2
    for j in range(2):
        for i in range(len(output[j])):
            assert list(output[j][i].keys()) == ['data']
