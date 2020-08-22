const form1 = document.getElementById("form");
const btn = document.getElementById("button");
const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;

let url;


async function form(event) {
    event.preventDefault();
    let key = document.getElementById('keys').value;
    let id = document.getElementById('id').value;
    let file = document.getElementById("myFile").files[0];

    const fd = new FormData()

    fd.append('id',id);
    fd.append(`${key}`,file);
    try {
        if(key === 'fc_model'){
            url = "http://127.0.0.1:8000/api_fc/";
        }
        else if (key === 'vc_model'){
            url = "http://127.0.0.1:8000/api_vc/";
        }
        else if (key === 'ncr_input'){
            url = "http://127.0.0.1:8000/api_ncr/";
        }
        else {
            throw new Error("Please Enter Valid Key!")
        }
        const send = await axios.post(url,fd, {
            headers: {
                'Content-Type': 'multipart/form-data',
                "X-CSRFToken" : csrfToken,
                }
        })
        .catch(error => {
            console.log(error);
            throw new Error(error);
          });
    }

    catch (error) {
        alert(error);
    }
}

btn.addEventListener('click', form)