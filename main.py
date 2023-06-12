import streamlit as st
from repos import JsonPatientRepo, SqlitePatientRepo, SqlitePatientRepoNative

# repo = JsonPatientRepo()
# repo = SqlitePatientRepo(path='patients.db')
repo = SqlitePatientRepoNative(path='patients2.db')
repo.create_db()

"# Patient Logger"

enter_tab, view_tab = st.tabs(["Enter Patient", "View/Edit Patients"])

with enter_tab:
    name = st.text_input(label='Name', placeholder="John Smith")
    age = st.slider('Age', 18, 31, step=1)
    submitted = st.button('Submit', on_click=lambda: repo.add_patient(name=name, age=age))

with view_tab:
    patients = repo.get_all_patients()
    if patients is not None:
        patients_ui = patients[['name', 'age', 'created_on']].sort_values('created_on').copy()
        patients_ui['Archive'] = False

        patients_edited = st.data_editor(patients_ui,num_rows='static', hide_index=True)

        do_archive = st.button('Archive')
        if do_archive:
            patients_to_archive = patients[patients_edited['Archive']]
            for _, patient in patients_to_archive.iterrows():
                repo.archive_patient(id=patient.id)
            st.experimental_rerun()
            
        