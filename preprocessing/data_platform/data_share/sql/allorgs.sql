(
    SELECT
        act.activity_id,
        act.assay_id,
        act.standard_type,
        CAST(act.standard_value AS DOUBLE PRECISION) as standard_value,
        act.standard_units,
        act.standard_relation,
        act.pchembl_value,
        act.data_validity_comment,
        act.activity_comment,
        act.potential_duplicate,
        act.bao_endpoint,
        a.assay_type,
        a.bao_format,
        a.confidence_score,
        td.pref_name as target_name,
        csq.organism,
        md.chembl_id,
        md.max_phase,
        cs.standard_inchi_key,
        cs.canonical_smiles,
        cp.mw_freebase,
        cp.alogp,
        cp.hba,
        cp.hbd,
        cp.psa,
        cp.rtb,
        cp.num_ro5_violations

    FROM activities act
    JOIN assays a ON act.assay_id = a.assay_id
    JOIN target_dictionary td ON a.tid = td.tid
    LEFT JOIN target_components tc ON td.tid = tc.tid
    LEFT JOIN component_sequences csq ON tc.component_id = csq.component_id
    JOIN molecule_dictionary md ON act.molregno = md.molregno
    JOIN compound_structures cs ON act.molregno = cs.molregno
    LEFT JOIN compound_properties cp ON md.molregno = cp.molregno
    WHERE
        act.standard_value IS NOT NULL
        {target_filter}
        {quality_filter}
        {limit_filter}
)