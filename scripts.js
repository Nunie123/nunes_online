
function showBio(node){
    node.classList.add('active');
    document.getElementById('resume-a').classList.remove('active');
    document.getElementById('bio-text').classList.remove('hidden');
    document.getElementById('resume-text').classList.add('hidden');
    return false;
}

function showResume(node){
    node.classList.add('active');
    document.getElementById('bio-a').classList.remove('active');
    document.getElementById('resume-text').classList.remove('hidden');
    document.getElementById('bio-text').classList.add('hidden');
    return false;
}